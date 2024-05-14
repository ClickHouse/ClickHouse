#!/usr/bin/env python3

IMAGES_ENV = {
    "altinityinfra/dotnet-client": "DOCKER_DOTNET_CLIENT_TAG",
    "altinityinfra/integration-helper": "DOCKER_HELPER_TAG",
    "altinityinfra/integration-test": "DOCKER_BASE_TAG",
    "altinityinfra/integration-tests-runner": "",
    "altinityinfra/kerberized-hadoop": "DOCKER_KERBERIZED_HADOOP_TAG",
    "altinityinfra/kerberos-kdc": "DOCKER_KERBEROS_KDC_TAG",
    "altinityinfra/mysql-golang-client": "DOCKER_MYSQL_GOLANG_CLIENT_TAG",
    "altinityinfra/mysql-java-client": "DOCKER_MYSQL_JAVA_CLIENT_TAG",
    "altinityinfra/mysql-js-client": "DOCKER_MYSQL_JS_CLIENT_TAG",
    "altinityinfra/mysql-php-client": "DOCKER_MYSQL_PHP_CLIENT_TAG",
    "altinityinfra/nginx-dav": "DOCKER_NGINX_DAV_TAG",
    "altinityinfra/postgresql-java-client": "DOCKER_POSTGRESQL_JAVA_CLIENT_TAG",
    "altinityinfra/python-bottle": "DOCKER_PYTHON_BOTTLE_TAG",
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
