# docker build -t clickhouse/style-test .
FROM ubuntu:22.04
ARG ACT_VERSION=0.2.33
ARG ACTIONLINT_VERSION=1.6.22

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        aspell \
        curl \
        git \
        file \
        libxml2-utils \
        moreutils \
        python3-fuzzywuzzy \
        python3-pip \
        yamllint \
        locales \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# python-magic is the same version as in Ubuntu 22.04
RUN pip3 install black==23.12.0 boto3 codespell==2.2.1 mypy==1.8.0 PyGithub unidiff pylint==3.1.0 \
        python-magic==0.4.24 requests types-requests \
    && rm -rf /root/.cache/pip

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH

ARG SHELLCHECK_VERSION=0.9.0
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) sarch=x86_64 ;; \
      arm64) sarch=aarch64 ;; \
    esac \
  && curl -L \
    "https://github.com/koalaman/shellcheck/releases/download/v${SHELLCHECK_VERSION}/shellcheck-v${SHELLCHECK_VERSION}.linux.${sarch}.tar.xz" \
    | tar xJ --strip=1 -C /tmp \
  && mv /tmp/shellcheck /usr/bin \
  && rm -rf /tmp/*


# Get act and actionlint from releases
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) act_arch=x86_64 ;; \
      arm64) act_arch=$arch ;; \
    esac \
  && curl -o /tmp/act.tgz -L \
    "https://github.com/nektos/act/releases/download/v${ACT_VERSION}/act_Linux_${act_arch}.tar.gz" \
  && tar xf /tmp/act.tgz -C /usr/bin act \
  && rm /tmp/act.tgz \
  && curl -o /tmp/actiolint.zip -L \
    "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION}_linux_${arch}.tar.gz" \
  && tar xf /tmp/actiolint.zip -C /usr/bin actionlint \
  && rm /tmp/actiolint.zip


COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
