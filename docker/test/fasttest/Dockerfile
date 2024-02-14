# rebuild in #33610
#  docker build -t clickhouse/fasttest .
ARG FROM_TAG=latest
FROM clickhouse/test-util:$FROM_TAG

RUN apt-get update \
    && apt-get install \
        brotli \
        expect \
        file \
        lsof \
        odbcinst \
        psmisc \
        python3 \
        python3-lxml \
        python3-pip \
        python3-requests \
        python3-termcolor \
        unixodbc \
        pv \
        jq \
        zstd \
        sudo \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN pip3 install numpy==1.26.3 scipy==1.12.0 pandas==1.5.3 Jinja2==3.1.3

RUN export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 3FA7E0328081BFF6A14DA29AA6A19B38D3D831EF \
    && echo "deb https://download.mono-project.com/repo/ubuntu stable-focal main" | sudo tee /etc/apt/sources.list.d/mono-official-stable.list \
    && apt update \
    && apt install mono-complete --yes \
    && apt clean \
    && rm -rfv /var/lib/apt/lists/*

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
  && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
  && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
  && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
  && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
  && rm -rf /tmp/clickhouse-odbc-tmp

# Give suid to gdb to grant it attach permissions
# chmod 777 to make the container user independent
RUN chmod u+s /usr/bin/gdb \
  && mkdir -p /var/lib/clickhouse \
  && chmod 777 /var/lib/clickhouse

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV COMMIT_SHA=''
ENV PULL_REQUEST_NUMBER=''
ENV COPY_CLICKHOUSE_BINARY_TO_OUTPUT=0

COPY run.sh /

CMD ["/bin/bash", "/run.sh"]
