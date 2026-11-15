# docker build -t clickhouse/kerberos-kdc .
FROM centos:6

RUN sed -i '/^mirrorlist/s/^/#/;/^#baseurl/{s/#//;s/mirror.centos.org\/centos\/$releasever/vault.centos.org\/6.10/}' /etc/yum.repos.d/*B*

RUN yum install -y ca-certificates krb5-server krb5-libs krb5-auth-dialog krb5-workstation

EXPOSE 88 749

RUN touch /config.sh
# should be overwritten e.g. via docker_compose volumes
#   volumes: /some_path/my_kerberos_config.sh:/config.sh:ro


ENTRYPOINT ["/bin/bash", "/config.sh"]
