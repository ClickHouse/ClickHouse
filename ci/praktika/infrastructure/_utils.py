from functools import lru_cache


def aws_client(service: str, region: str, context: str = "", **kwargs):
    """Create a boto3 client with an explicit region — never falls back to boto3 default.

    Raises ValueError if region is empty so misconfigured deployments fail
    immediately with a clear message instead of silently using the wrong region.

    If Settings.AWS_PROFILE is set, a named session is used so SSO profiles
    work without requiring AWS_PROFILE to be exported in the shell.
    """
    if not region:
        who = f" (context: {context})" if context else ""
        raise ValueError(
            f"AWS region is not set{who}. "
            f"Configure Settings.AWS_REGION in your ci/settings/*.py file."
        )
    import boto3
    from ..settings import Settings
    if Settings.AWS_PROFILE:
        session = boto3.Session(profile_name=Settings.AWS_PROFILE, region_name=region)
        return session.client(service, **kwargs)
    return boto3.client(service, region_name=region, **kwargs)


@lru_cache(maxsize=16)
def aws_account_id(region: str) -> str:
    """Return the AWS account id for the current credentials."""
    sts = aws_client("sts", region, "account-id")
    return sts.get_caller_identity()["Account"]
