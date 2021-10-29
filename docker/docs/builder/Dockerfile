# docker build -t clickhouse/docs-build .
FROM ubuntu:20.04

ENV LANG=C.UTF-8

RUN sed -i 's|http://archive|http://ru.archive|g' /etc/apt/sources.list

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            python3-setuptools \
            virtualenv \
            wget \
            bash \
            python \
            curl \
            python3-requests \
            sudo \
            git \
            openssl \
            python3-pip \
            software-properties-common \
            language-pack-zh* \
            chinese* \
            fonts-arphic-ukai \
            fonts-arphic-uming \
            fonts-ipafont-mincho \
            fonts-ipafont-gothic \
            fonts-unfonts-core \
            xvfb \
            nodejs \
            npm \
            openjdk-11-jdk \
    && pip --no-cache-dir install scipy \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN wget 'https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.focal_amd64.deb'

RUN npm i -g purify-css

RUN pip3 install Babel==2.8.0 backports-abc==0.5 backports.functools-lru-cache==1.6.1 beautifulsoup4==4.9.1 Pygments>=2.7.4 \
certifi==2020.4.5.2 chardet==3.0.4 click==7.1.2 closure==20191111 cssmin==0.2.0 future==0.18.2 htmlmin==0.1.12 \
idna==2.10 Jinja2>=2.11.3 jinja2-highlight==0.6.1 jsmin==3.0.0 livereload==2.6.2 Markdown==3.3.2 MarkupSafe==2.0.1 \
mkdocs==1.2.3 mkdocs-htmlproofer-plugin==0.0.3 mkdocs-macros-plugin==0.4.20 nltk==3.5 nose==1.3.7 protobuf==3.14.0 \
numpy==1.21.2 pymdown-extensions==9.0 python-slugify==4.0.1 PyYAML==5.4.1 repackage==0.7.3 requests==2.25.1 \
singledispatch==3.4.0.3 six==1.15.0 soupsieve==2.0.1 termcolor==1.1.0 tornado==6.1 Unidecode==1.1.1 urllib3>=1.26.5
