from dataclasses import dataclass, field
from typing import Any, Dict, List

from praktika.infrastructure.autoscaling_group import AutoScalingGroup
from praktika.infrastructure.iam_instance_profile import IAMInstanceProfile
from praktika.infrastructure.iam_role import IAMRole
from praktika.infrastructure.launch_template import LaunchTemplate

from .configs import S3_PROXY_INSTANCE_PROFILE_NAME, S3_PROXY_ROLE_NAME
from .user_data import s3_proxy_user_data


def _ssm_parameter_arn(name: str) -> str:
    value = name.strip()
    if value.startswith("arn:"):
        return value
    return f"arn:aws:ssm:*:*:parameter/{value.lstrip('/')}"


def _bucket_read_arns(buckets: List[str]) -> List[str]:
    arns: List[str] = []
    for bucket in buckets:
        bucket = (bucket or "").strip()
        if not bucket:
            continue
        arns.append(f"arn:aws:s3:::{bucket}")
        arns.append(f"arn:aws:s3:::{bucket}/*")
    return arns


@dataclass
class S3Proxy:
    """A self-healing Tailscale reverse proxy that serves the project's PRIVATE
    S3 report buckets to tailnet users, read-only.

    Shape: a single-instance AutoScalingGroup (min=max=desired=1) so a dead node
    is replaced automatically, plus the LaunchTemplate, IAM role, and instance
    profile it needs. All are created at construction time and registered into
    CloudInfrastructure.Config automatically via its ``s3_proxy`` field.

    Data path (see s3_proxy_user_data.sh)::

        Tailscale user --[tailnet]--> Caddy (:443 TLS, :8080 HTTP, GET/HEAD only)
                                        --> signer.py (SigV4, EC2 instance role)
                                        --> S3 (buckets stay fully private)

    The buckets keep ``public=False``: the proxy reads them with the EC2
    instance role (SigV4-signed by a local signing sidecar), so no anonymous
    access or bucket-policy change is required. The instance role is granted
    read-only S3 on exactly ``proxied_buckets`` — when left empty,
    CloudInfrastructure fills it with every project ``Storage`` bucket.

    Tailnet join is credential-free at rest: the node mints an ephemeral,
    tagged auth key at boot from a Tailscale OAuth client stored in SSM
    (``tailscale_oauth_client_id_ssm`` / ``tailscale_oauth_client_secret_ssm``,
    created out of band). The node registers under ``hostname``, so its report
    URL is the stable ``https://{hostname}.<tailnet>.ts.net/{bucket}/{key}``.

    Example::

        s3_proxy = S3Proxy(
            hostname="my-project-ci-reports",
            tailscale_tag="tag:ci-s3-proxy",
        )
    """

    name: str = "s3-proxy"
    instance_type: str = "t4g.micro"
    vpc_name: str = ""
    ami_id: str = ""  # resolved at deploy time via SSM if empty
    # Tailscale node hostname; defaults to `name`. Must be unique on the
    # tailnet, so project scaffolding sets it to "{slug}-ci-reports".
    hostname: str = ""
    tailscale_tag: str = "tag:ci-s3-proxy"
    tailscale_oauth_client_id_ssm: str = "/praktika/tailscale/oauth-client-id"
    tailscale_oauth_client_secret_ssm: str = "/praktika/tailscale/oauth-client-secret"
    # Buckets to serve. Empty => filled with every project Storage bucket by
    # CloudInfrastructure (after project namespacing) via set_proxied_buckets().
    proxied_buckets: List[str] = field(default_factory=list)
    security_group_ids: List[str] = field(default_factory=list)
    security_group_names: List[str] = field(default_factory=list)
    volume_size_gb: int = 20
    region: str = ""
    ext: Dict[str, Any] = field(default_factory=dict)

    ec2_role: IAMRole.Config = field(init=False)
    instance_profile: IAMInstanceProfile.Config = field(init=False)
    launch_template: LaunchTemplate.Config = field(init=False)
    autoscaling_group: AutoScalingGroup.Config = field(init=False)

    def __post_init__(self):
        if not self.hostname:
            self.hostname = self.name
        if (
            self.vpc_name
            and not self.security_group_ids
            and not self.security_group_names
        ):
            self.security_group_names = [f"{self.vpc_name}-sg"]

        self.ec2_role = IAMRole.Config(
            name=S3_PROXY_ROLE_NAME,
            trust_service="ec2.amazonaws.com",
            policy_arns=[
                "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
            ],
            inline_policies={},
        )
        self.instance_profile = IAMInstanceProfile.Config(
            name=S3_PROXY_INSTANCE_PROFILE_NAME,
            role_name=self.ec2_role.name,
        )
        self.launch_template = LaunchTemplate.Config(
            name=f"{self.name}-lt",
            image_id=self.ami_id,
            instance_type=self.instance_type,
            security_group_ids=self.security_group_ids,
            security_group_names=self.security_group_names,
            vpc_name=self.vpc_name,
            iam_instance_profile_name=self.instance_profile.name,
            set_default_version_to_latest=True,
            user_data="",  # rendered by _refresh() below
            root_volume_size_gb=self.volume_size_gb,
            root_volume_type="gp3",
            tags={"praktika_role": "s3_proxy"},
            praktika_resource_tag="s3-proxy",
        )
        self.autoscaling_group = AutoScalingGroup.Config(
            name=self.name,
            vpc_name=self.vpc_name,
            availability_zones=[],
            min_size=1,
            max_size=1,
            desired_capacity=1,
            launch_template_name=self.launch_template.name,
            launch_template_version="$Latest",
            tags={"praktika_role": "s3_proxy"},
            praktika_resource_tag="s3-proxy",
        )
        self._refresh()

    def _refresh(self):
        """(Re)derive the S3 IAM statement and user_data from the current
        hostname + proxied_buckets. Called at construction and again by
        CloudInfrastructure after project namespacing resolves both."""
        statements = [
            {
                "Sid": "ReadTailscaleOAuthClient",
                "Effect": "Allow",
                "Action": ["ssm:GetParameter", "ssm:GetParameters"],
                "Resource": [
                    _ssm_parameter_arn(self.tailscale_oauth_client_id_ssm),
                    _ssm_parameter_arn(self.tailscale_oauth_client_secret_ssm),
                ],
            }
        ]
        bucket_arns = _bucket_read_arns(self.proxied_buckets)
        if bucket_arns:
            statements.append(
                {
                    "Sid": "ReadProxiedBuckets",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectTagging",
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                    ],
                    "Resource": bucket_arns,
                }
            )
        self.ec2_role.inline_policies = {
            "S3ProxyAccess": {
                "Version": "2012-10-17",
                "Statement": statements,
            }
        }
        self.launch_template.user_data = s3_proxy_user_data(
            hostname=self.hostname,
            tailscale_tag=self.tailscale_tag,
            oauth_client_id_ssm=self.tailscale_oauth_client_id_ssm,
            oauth_client_secret_ssm=self.tailscale_oauth_client_secret_ssm,
            proxied_buckets=self.proxied_buckets,
        )

    def set_proxied_buckets(self, buckets: List[str]):
        """Set the served buckets (post-namespacing) and re-derive the IAM
        policy + user_data. No-op if the caller passes an empty list."""
        cleaned = [b for b in (buckets or []) if b and b.strip()]
        if not cleaned:
            return
        self.proxied_buckets = cleaned
        self._refresh()
