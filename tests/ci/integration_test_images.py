#!/usr/bin/env python3

IMAGES_ENV = {
    "clickhouse/dotnet-client": "DOCKER_DOTNET_CLIENT_TAG",
    "clickhouse/integration-helper": "DOCKER_HELPER_TAG",
    "clickhouse/integration-test": "DOCKER_BASE_TAG",
    "clickhouse/integration-tests-runner": "",
    "clickhouse/kerberized-hadoop": "DOCKER_KERBERIZED_HADOOP_TAG",
    "clickhouse/kerberos-kdc": "DOCKER_KERBEROS_KDC_TAG",
    "clickhouse/mysql-golang-client": "DOCKER_MYSQL_GOLANG_CLIENT_TAG",
    "clickhouse/mysql-java-client": "DOCKER_MYSQL_JAVA_CLIENT_TAG",
    "clickhouse/mysql-js-client": "DOCKER_MYSQL_JS_CLIENT_TAG",
    "clickhouse/mysql-php-client": "DOCKER_MYSQL_PHP_CLIENT_TAG",
    "clickhouse/nginx-dav": "DOCKER_NGINX_DAV_TAG",
    "clickhouse/postgresql-java-client": "DOCKER_POSTGRESQL_JAVA_CLIENT_TAG",
    "clickhouse/python-bottle": "DOCKER_PYTHON_BOTTLE_TAG",
}

IMAGES = list(IMAGES_ENV.keys())


def get_image_env(image: str) -> str:
    return IMAGES_ENV.get(image, "")


def get_docker_env(image: str, tag: str) -> str:
    "if image belongs to IMAGES_ENV, return `-e` argument for docker command"
    env = get_image_env(image)
    if not env:
        return env
    return f"-e {env}={tag} "
