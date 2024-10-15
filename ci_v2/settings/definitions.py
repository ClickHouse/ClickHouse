from praktika import Docker, Secret

S3_BUCKET_NAME = "clickhouse-builds"
S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"


class RunnerLabels:
    CI_SERVICES = "ci_services"
    CI_SERVICES_EBS = "ci_services_ebs"


BASE_BRANCH = "master"

SECRETS = [
    Secret.Config(
        name="dockerhub_robot_password",
        type=Secret.Type.AWS_SSM_VAR,
    ),
    Secret.Config(
        name="woolenwolf_gh_app.clickhouse-app-id",
        type=Secret.Type.AWS_SSM_SECRET,
    ),
    Secret.Config(
        name="woolenwolf_gh_app.clickhouse-app-key",
        type=Secret.Type.AWS_SSM_SECRET,
    ),
]

DOCKERS = [
    # Docker.Config(
    #     name="clickhouse/binary-builder",
    #     path="./docker/packager/binary-builder",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/cctools",
    #     path="./docker/packager/cctools",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/test-old-centos",
    #     path="./docker/test/compatibility/centos",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/test-old-ubuntu",
    #     path="./docker/test/compatibility/ubuntu",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/test-util",
    #     path="./docker/test/util",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/integration-test",
    #     path="./docker/test/integration/base",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/fuzzer",
    #     path="./docker/test/fuzzer",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/performance-comparison",
    #     path="./docker/test/performance-comparison",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/fasttest",
    #     path="./docker/test/fasttest",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-util"],
    # ),
    # Docker.Config(
    #     name="clickhouse/test-base",
    #     path="./docker/test/base",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-util"],
    # ),
    # Docker.Config(
    #     name="clickhouse/clickbench",
    #     path="./docker/test/clickbench",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/keeper-jepsen-test",
    #     path="./docker/test/keeper-jepsen",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/server-jepsen-test",
    #     path="./docker/test/server-jepsen",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/sqllogic-test",
    #     path="./docker/test/sqllogic",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/sqltest",
    #     path="./docker/test/sqltest",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/stateless-test",
    #     path="./docker/test/stateless",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/stateful-test",
    #     path="./docker/test/stateful",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/stateless-test"],
    # ),
    # Docker.Config(
    #     name="clickhouse/stress-test",
    #     path="./docker/test/stress",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/stateful-test"],
    # ),
    # Docker.Config(
    #     name="clickhouse/unit-test",
    #     path="./docker/test/unit",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/integration-tests-runner",
    #     path="./docker/test/integration/runner",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
    Docker.Config(
        name="clickhouse/style-test",
        path="./ci_v2/docker/style-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    # Docker.Config(
    #     name="clickhouse/docs-builder",
    #     path="./docker/docs/builder",
    #     arm64=True,
    #     amd64=True,
    #     depends_on=["clickhouse/test-base"],
    # ),
]

# TODO:
# "docker/test/integration/s3_proxy": {
#     "name": "clickhouse/s3-proxy",
#     "dependent": []
# },
# "docker/test/integration/resolver": {
#     "name": "clickhouse/python-bottle",
#     "dependent": []
# },
# "docker/test/integration/helper_container": {
#     "name": "clickhouse/integration-helper",
#     "dependent": []
# },
# "docker/test/integration/mysql_golang_client": {
#     "name": "clickhouse/mysql-golang-client",
#     "dependent": []
# },
# "docker/test/integration/dotnet_client": {
#     "name": "clickhouse/dotnet-client",
#     "dependent": []
# },
# "docker/test/integration/mysql_java_client": {
#     "name": "clickhouse/mysql-java-client",
#     "dependent": []
# },
# "docker/test/integration/mysql_js_client": {
#     "name": "clickhouse/mysql-js-client",
#     "dependent": []
# },
# "docker/test/integration/mysql_php_client": {
#     "name": "clickhouse/mysql-php-client",
#     "dependent": []
# },
# "docker/test/integration/postgresql_java_client": {
#     "name": "clickhouse/postgresql-java-client",
#     "dependent": []
# },
# "docker/test/integration/kerberos_kdc": {
#     "only_amd64": true,
#     "name": "clickhouse/kerberos-kdc",
#     "dependent": []
# },
# "docker/test/integration/kerberized_hadoop": {
#     "only_amd64": true,
#     "name": "clickhouse/kerberized-hadoop",
#     "dependent": []
# },
# "docker/test/sqlancer": {
#     "name": "clickhouse/sqlancer-test",
#     "dependent": []
# },
# "docker/test/install/deb": {
#     "name": "clickhouse/install-deb-test",
#     "dependent": []
# },
# "docker/test/install/rpm": {
#     "name": "clickhouse/install-rpm-test",
#     "dependent": []
# },
# "docker/test/integration/nginx_dav": {
#     "name": "clickhouse/nginx-dav",
#     "dependent": []
# }


class JobNames:
    STYLE_CHECK = "Style Check"
