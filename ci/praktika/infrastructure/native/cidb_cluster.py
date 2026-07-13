from dataclasses import dataclass, field
from typing import Any, Dict, List

from praktika.infrastructure.ec2_instance import EC2Instance
from praktika.infrastructure.iam_instance_profile import IAMInstanceProfile
from praktika.infrastructure.iam_role import IAMRole
from praktika.infrastructure.secret_parameter import SecretParameter

from .configs import (
    CIDB_ADMIN_PASSWORD_SECRET_NAME,
    CIDB_INSTANCE_PROFILE_NAME,
    CIDB_ROLE_NAME,
)
from .user_data import cidb_user_data


@dataclass
class CIDBCluster:
    """A self-contained CI DB cluster: OSS ClickHouse + embedded Keeper on EC2.

    Single-node only for now (size=1). Each replica runs a colocated Keeper
    so the schema's ReplicatedMergeTree engine works out of the box and a
    future multi-node deploy can join the same Keeper quorum without
    recreating tables.

    Auth model (configured via user_data):
      - Praktika runners write without a password — restricted by VPC CIDR
        on the ClickHouse `<networks>` ACL plus the EC2 security group.
      - An ``admin`` user takes a password from SSM (auto-generated on first
        deploy) and is intended for human/Tailscale access bridged into
        the VPC.

    Persistence:
      - Separate gp3 EBS data volume mounted at /var/lib/clickhouse on first
        boot. ``DeleteOnTermination=False`` so accidental termination leaves
        an orphan volume rather than wiping data.

    Example::

        cidb = CIDBCluster(
            vpc_name="project-ci",
            instance_type="t4g.large",
            size=1,
        )
    """

    vpc_name: str = ""
    instance_type: str = "t4g.large"
    size: int = 1
    ami_id: str = ""  # resolved at deploy time via SSM if empty
    iam_instance_profile_name: str = CIDB_INSTANCE_PROFILE_NAME
    ec2_role_name: str = CIDB_ROLE_NAME
    admin_password_secret_name: str = CIDB_ADMIN_PASSWORD_SECRET_NAME
    security_group_ids: List[str] = field(default_factory=list)
    security_group_names: List[str] = field(default_factory=list)
    root_volume_size_gb: int = 30
    data_volume_size_gb: int = 100
    # CIDR that the runner/admin users are allowed to connect from (must
    # cover the praktika VPC). Used to render the ClickHouse `<networks>` ACL.
    vpc_cidr: str = "10.0.0.0/16"
    region: str = ""
    ext: Dict[str, Any] = field(default_factory=dict)

    ec2_role: IAMRole.Config = field(init=False)
    instance_profile: IAMInstanceProfile.Config = field(init=False)
    admin_password_secret: SecretParameter.Config = field(init=False)
    instances: List[EC2Instance.Config] = field(init=False)

    def __post_init__(self):
        if self.size != 1:
            raise NotImplementedError(
                f"CIDBCluster.size={self.size} is not yet supported; only size=1 is implemented."
            )
        if (
            self.vpc_name
            and not self.security_group_ids
            and not self.security_group_names
        ):
            self.security_group_names = [f"{self.vpc_name}-sg"]

        self.admin_password_secret = SecretParameter.Config(
            name=self.admin_password_secret_name,
            description="Praktika CI DB admin user password",
            generate_random=True,
        )

        secret_name_for_arn = self.admin_password_secret_name.lstrip("/")
        self.ec2_role = IAMRole.Config(
            name=self.ec2_role_name,
            trust_service="ec2.amazonaws.com",
            policy_arns=[
                "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
                "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy",
            ],
            inline_policies={
                "CIDBAccess": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "ReadAdminPassword",
                            "Effect": "Allow",
                            "Action": [
                                "ssm:GetParameter",
                                "ssm:GetParameters",
                            ],
                            "Resource": [
                                f"arn:aws:ssm:*:*:parameter/{secret_name_for_arn}",
                            ],
                        },
                        {
                            "Sid": "CloudWatchLogs",
                            "Effect": "Allow",
                            "Action": [
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            "Resource": "arn:aws:logs:*:*:*",
                        },
                    ],
                },
            },
        )
        self.instance_profile = IAMInstanceProfile.Config(
            name=self.iam_instance_profile_name,
            role_name=self.ec2_role_name,
        )

        self.instances = []
        for index in range(1, self.size + 1):
            replica_name = f"cidb-{index:02d}"
            user_data = cidb_user_data(
                vpc_cidr=self.vpc_cidr,
                admin_password_ssm_name=self.admin_password_secret_name,
                replica_name=replica_name,
            )
            # /dev/sdf is conventional for the first non-root EBS on Linux.
            # On Nitro instances the kernel surfaces it as /dev/nvme1n1; the
            # bootstrap script discovers it dynamically.
            data_volume_mapping = {
                "DeviceName": "/dev/sdf",
                "Ebs": {
                    "VolumeSize": int(self.data_volume_size_gb),
                    "VolumeType": "gp3",
                    "DeleteOnTermination": False,
                },
            }
            self.instances.append(
                EC2Instance.Config(
                    name=replica_name,
                    region=self.region,
                    quantity=1,
                    praktika_resource_tag="cidb",
                    image_id=self.ami_id,
                    instance_type=self.instance_type,
                    security_group_ids=list(self.security_group_ids),
                    iam_instance_profile_name=self.iam_instance_profile_name,
                    user_data=user_data,
                    root_volume_size=self.root_volume_size_gb,
                    root_volume_type="gp3",
                    extra_block_device_mappings=[data_volume_mapping],
                    start_on_deploy=True,
                )
            )

    def deploy(self):
        """Deploy SG ingress for ClickHouse ports, then each replica instance.

        The IAM role/profile and the admin password secret are deployed via
        their normal CloudInfrastructure passes; this method handles only
        what's specific to the cluster (SG rules + per-replica EC2 launch).
        """
        from .._utils import aws_client
        from ..vpc import VPC
        from .configs import resolve_al2023_arm64_ami, resolve_al2023_x86_64_ami

        if not self.region:
            raise ValueError("CIDBCluster.region is not set")

        # EC2Instance.deploy() requires an explicit image_id. Mirror
        # LaunchTemplate._resolve_image_id: pick the latest AL2023 AMI for
        # the instance family architecture (Graviton families end in 'g').
        if not self.ami_id:
            family = (self.instance_type or "").split(".")[0]
            is_arm = family.endswith("g")
            self.ami_id = (
                resolve_al2023_arm64_ami(self.region)
                if is_arm
                else resolve_al2023_x86_64_ami(self.region)
            )

        lookup = VPC.Lookup(name=self.vpc_name, region=self.region)
        subnet_id = lookup.first_subnet_id()
        sg_ids = list(self.security_group_ids)
        sg_ids.extend(lookup.resolve_security_group_ids(self.security_group_names))
        if not sg_ids:
            raise ValueError("CIDBCluster has no resolvable security groups")

        # Authorize CH HTTP (8123) and native (9000) ingress, source = same SG.
        # Idempotent: ignore InvalidPermission.Duplicate.
        ec2 = aws_client("ec2", self.region, "cidb-cluster")
        for sg_id in sg_ids:
            for port in (8123, 9000):
                try:
                    ec2.authorize_security_group_ingress(
                        GroupId=sg_id,
                        IpPermissions=[
                            {
                                "IpProtocol": "tcp",
                                "FromPort": port,
                                "ToPort": port,
                                "UserIdGroupPairs": [{"GroupId": sg_id}],
                            }
                        ],
                    )
                    print(f"Authorized tcp/{port} on {sg_id} from {sg_id}")
                except ec2.exceptions.ClientError as e:
                    if "InvalidPermission.Duplicate" in str(e):
                        print(f"Ingress tcp/{port} on {sg_id} already present")
                    else:
                        raise

        # Push resolved AMI/SG/subnet onto each replica's instance config.
        for instance in self.instances:
            instance.region = self.region
            if not instance.image_id:
                instance.image_id = self.ami_id
            if not instance.security_group_ids:
                instance.security_group_ids = sg_ids
            if not instance.subnet_id:
                instance.subnet_id = subnet_id
            print("\n" + "=" * 60)
            print(f"Deploying CIDB instance: {instance.name}")
            print("=" * 60)
            instance.deploy()

        self._publish_url_to_ssm()

    def _publish_url_to_ssm(self):
        """Write a JSON connection blob into the SSM parameter that runners
        read at job time (Settings.SECRET_CI_DB_CONNECTION points to it).

        The blob has the shape ``{"url", "user", "password"}`` so a single
        secret carries everything CIDB.from_connection_secret() needs.
        ``user`` must name the `runner` CH user explicitly: ClickHouse has
        no anonymous mode and falls back to `default` when no
        X-ClickHouse-User header is sent, and we lock `default` to
        localhost so a runner-IP request would hit 401. ``password`` is
        null because the runner user is configured ``<no_password/>`` —
        any value (including an absent header) is accepted.
        """
        import json as _json

        from .._utils import aws_client
        from ...settings import Settings

        param_name = Settings.SECRET_CI_DB_CONNECTION
        if not param_name:
            print(
                "Settings.SECRET_CI_DB_CONNECTION is unset — skip publishing CIDB "
                "connection (set it to an SSM parameter name to enable auto-publish)"
            )
            return

        instance = self.instances[0]
        instance.fetch()
        ip = instance.ext.get("private_ip")
        if not ip:
            print(
                f"WARNING: CIDB instance {instance.name} has no private IP yet — "
                f"skip publishing connection to SSM '{param_name}'"
            )
            return

        connection = {
            "url": f"http://{ip}:8123",
            "user": "runner",
            "password": None,
        }
        ssm = aws_client("ssm", self.region, "cidb-connection")
        ssm.put_parameter(
            Name=param_name,
            Value=_json.dumps(connection),
            Type="String",
            Overwrite=True,
            Description="Praktika CI DB connection JSON (auto-published by CIDBCluster.deploy)",
        )
        print(
            f"Published CIDB connection {connection} to SSM parameter '{param_name}'"
        )
