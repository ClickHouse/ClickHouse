"""Helpers that build project-namespaced IAM resource ARNs.

All Praktika AWS resources for a project share the ``{project-slug}-``
name prefix (queues, ASGs, roles, the artifact bucket, and—by
convention—the project's secrets and SSM parameters). These helpers
turn that prefix into resource ARNs so instance and Lambda roles can be
scoped to the project's own resources instead of the whole account.

Account and region are intentionally wildcarded (``*:*``): instance-role
credentials only work within their own account, so the resource-*name*
prefix is the meaningful tenant boundary, and wildcarding survives
account/region moves without re-pinning policies. ``PROJECT_SLUG`` is
required: rather than silently widening a scoped policy back to the whole
account, a missing slug fails the deploy with an actionable error.
"""

from typing import List

from praktika.settings import Settings


def project_slug() -> str:
    """Project namespace prefix used for all of this project's resources.

    Required: IAM scoping is built from this slug, so an empty value would
    silently widen every scoped policy back to the whole account. Fail the
    deploy loudly instead, and tell the operator how to fix it.
    """
    slug = (getattr(Settings, "PROJECT_SLUG", "") or "").strip()
    if not slug:
        raise ValueError(
            "Settings.PROJECT_SLUG is required to scope IAM policies to this "
            "project's resources. Set PROJECT_SLUG in ci/settings/settings.py "
            "(it should match the project/repo name, e.g. PROJECT_SLUG = "
            '"my_project").'
        )
    if "-" in slug:
        raise ValueError(
            f"Settings.PROJECT_SLUG={slug!r} must not contain '-'; use '_' "
            'instead (e.g. "clickhouse_private"). The slug is used as the '
            '"{slug}-" resource-name prefix, so a "-" inside it would let this '
            "project's scoped IAM wildcard also match another project's "
            'resources (e.g. "clickhouse-*" would match "clickhouse-private-*").'
        )
    return slug


def _name_prefixes() -> List[str]:
    """Name-prefix forms that count as "starts with the project slug".

    Covers both the dash convention Praktika uses for its own resources
    (``slug-name``) and the path convention often used for user-created
    secrets/parameters (``slug/name``).
    """
    slug = project_slug()
    return [f"{slug}-", f"{slug}/"]


def sqs_queue_arns() -> List[str]:
    # SQS queue names cannot contain "/", so only the dash form applies.
    return [f"arn:aws:sqs:*:*:{project_slug()}-*"]


def autoscaling_group_arns() -> List[str]:
    slug = project_slug()
    return [f"arn:aws:autoscaling:*:*:autoScalingGroup:*:autoScalingGroupName/{slug}-*"]


def secret_arns() -> List[str]:
    return [f"arn:aws:secretsmanager:*:*:secret:{p}*" for p in _name_prefixes()]


def ssm_parameter_arns() -> List[str]:
    return [f"arn:aws:ssm:*:*:parameter/{p}*" for p in _name_prefixes()]


def cloudwatch_log_group_arns() -> List[str]:
    # Praktika instances log under "/{slug}/..." (see the CloudWatch agent
    # config in native/image_builder.py). The ":*" variant covers the
    # log-stream sub-resources of those groups.
    slug = project_slug()
    return [
        f"arn:aws:logs:*:*:log-group:/{slug}/*",
        f"arn:aws:logs:*:*:log-group:/{slug}/*:*",
    ]


def project_bucket_arns() -> List[str]:
    bucket = (getattr(Settings, "S3_ARTIFACT_BUCKET", "") or "").strip()
    if not bucket:
        raise ValueError(
            "Settings.S3_ARTIFACT_BUCKET is required to scope S3 IAM policies "
            "to this project's bucket. Set it in ci/settings/settings.py."
        )
    return [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]
