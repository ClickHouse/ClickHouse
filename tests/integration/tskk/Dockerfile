FROM centos:6.6

# based on confluent kerberos, which is not provided as image any more

RUN yum install -y krb5-server krb5-libs krb5-auth-dialog krb5-workstation

EXPOSE 88 749

ADD ./kerberos_image_config.sh /config.sh

ENTRYPOINT ["/bin/bash", "/config.sh"]
