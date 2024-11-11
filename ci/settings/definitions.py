from praktika import Docker, Secret

S3_BUCKET_NAME = "clickhouse-builds"
S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"


class RunnerLabels:
    CI_SERVICES = "ci_services"
    CI_SERVICES_EBS = "ci_services_ebs"
    BUILDER = "builder"


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
    #     path="./ci/docker/packager/binary-builder",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/cctools",
    #     path="./ci/docker/packager/cctools",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/test-old-centos",
    #     path="./ci/docker/test/compatibility/centos",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/test-old-ubuntu",
    #     path="./ci/docker/test/compatibility/ubuntu",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/test-util",
    #     path="./ci/docker/test/util",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    # Docker.Config(
    #     name="clickhouse/integration-test",
    #     path="./ci/docker/test/integration/base",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/fuzzer",
    #     path="./ci/docker/test/fuzzer",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/performance-comparison",
    #     path="./ci/docker/test/performance-comparison",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=[],
    # ),
    Docker.Config(
        name="clickhouse/fasttest",
        path="./ci/docker/fasttest",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    # Docker.Config(
    #     name="clickhouse/test-base",
    #     path="./ci/docker/test/base",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-util"],
    # ),
    # Docker.Config(
    #     name="clickhouse/clickbench",
    #     path="./ci/docker/test/clickbench",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/keeper-jepsen-test",
    #     path="./ci/docker/test/keeper-jepsen",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/server-jepsen-test",
    #     path="./ci/docker/test/server-jepsen",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/sqllogic-test",
    #     path="./ci/docker/test/sqllogic",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/sqltest",
    #     path="./ci/docker/test/sqltest",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/stateless-test",
    #     path="./ci/docker/test/stateless",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/stateful-test",
    #     path="./ci/docker/test/stateful",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/stateless-test"],
    # ),
    # Docker.Config(
    #     name="clickhouse/stress-test",
    #     path="./ci/docker/test/stress",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/stateful-test"],
    # ),
    # Docker.Config(
    #     name="clickhouse/unit-test",
    #     path="./ci/docker/test/unit",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    # Docker.Config(
    #     name="clickhouse/integration-tests-runner",
    #     path="./ci/docker/test/integration/runner",
    #     platforms=Docker.Platforms.arm_amd,
    #     depends_on=["clickhouse/test-base"],
    # ),
    Docker.Config(
        name="clickhouse/style-test",
        path="./ci/docker/style-test",
        platforms=Docker.Platforms.arm_amd,
        depends_on=[],
    ),
    # Docker.Config(
    #     name="clickhouse/docs-builder",
    #     path="./ci/docker/docs/builder",
    #     platforms=Docker.Platforms.arm_amd,
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
    FAST_TEST = "Fast test"
    BUILD_AMD_DEBUG = "Build amd64 debug"
