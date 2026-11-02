# docker build -t clickhouse/style-test .
FROM ubuntu:22.04

# Redirect apt to the mirror passed by CI (canonical default; see ci/praktika/docker.py).
# Rewrite archive.ubuntu.com (amd64) and ports.ubuntu.com (arm64) across sources.list
# and deb822 *.sources so arm64 builds avoid the flaky ports.ubuntu.com.
ARG apt_archive="http://archive.ubuntu.com"
ARG apt_ports_archive="http://ports.ubuntu.com"
RUN find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -print0 | xargs -0 -r sed -i \
        -e "s|http://archive.ubuntu.com|$apt_archive|g" \
        -e "s|https://archive.ubuntu.com|$apt_archive|g" \
        -e "s|http://ports.ubuntu.com|$apt_ports_archive|g" \
        -e "s|https://ports.ubuntu.com|$apt_ports_archive|g"

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        curl \
        git \
        libxml2-utils \
        locales \
        python3-pip \
        ripgrep \
        zstd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt
