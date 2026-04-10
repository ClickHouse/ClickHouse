from praktika import CloudInfrastructure

CLOUD = CloudInfrastructure.Config(
    name="cloud_infra", lambda_functions=[*CloudInfrastructure.SLACK_APP_LAMBDAS]
)
