# docker build -t yandex/clickhouse-kerberos-kdc .

FROM centos:6.6
# old OS to make is faster and smaller

RUN yum install -y krb5-server krb5-libs krb5-auth-dialog krb5-workstation

EXPOSE 88 749

RUN touch /config.sh
# should be overwritten e.g. via docker_compose volumes
#   volumes: /some_path/my_kerberos_config.sh:/config.sh:ro


ENTRYPOINT ["/bin/bash", "/config.sh"]
