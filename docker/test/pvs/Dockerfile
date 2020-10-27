# docker build -t yandex/clickhouse-pvs-test .

FROM yandex/clickhouse-binary-builder

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            bash \
            sudo \
            wget \
            software-properties-common \
            gpg-agent \
            strace

#RUN wget -q -O - http://files.viva64.com/etc/pubkey.txt | sudo apt-key add -
#RUN sudo wget -O /etc/apt/sources.list.d/viva64.list http://files.viva64.com/etc/viva64.list
#
#RUN apt-get --allow-unauthenticated update -y \
#    && env DEBIAN_FRONTEND=noninteractive \
#        apt-get --allow-unauthenticated install --yes --no-install-recommends \
#            pvs-studio

ENV PKG_VERSION="pvs-studio-7.08.39365.50-amd64.deb"

RUN wget "https://files.viva64.com/$PKG_VERSION"
RUN sudo dpkg -i "$PKG_VERSION"

CMD echo "Running PVS version $PKG_VERSION" && cd /repo_folder && pvs-studio-analyzer credentials $LICENCE_NAME $LICENCE_KEY -o ./licence.lic  \
    && cmake . && ninja re2_st && \
    pvs-studio-analyzer analyze -o pvs-studio.log -e contrib -j 4 -l ./licence.lic; \
    plog-converter -a GA:1,2 -t fullhtml -o /test_output/pvs-studio-html-report pvs-studio.log; \
    plog-converter -a GA:1,2 -t tasklist -o /test_output/pvs-studio-task-report.txt pvs-studio.log

