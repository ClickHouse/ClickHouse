# docker build --network=host -t yandex/clickhouse-codebrowser .
# docker run --volume=path_to_repo:/repo_folder --volume=path_to_result:/test_output yandex/clickhouse-codebrowser
FROM ubuntu:18.04

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
        bash \
        sudo \
        wget \
        software-properties-common \
        ca-certificates \
        apt-transport-https \
        build-essential \
        gpg-agent \
        git

RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | sudo apt-key add -
RUN sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main'
RUN sudo echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list

RUN sudo apt-get --yes --allow-unauthenticated update
# To build woboq
RUN sudo apt-get --yes --allow-unauthenticated install cmake clang-8 libllvm8 libclang-8-dev

# repo versions doesn't work correctly with C++17
# also we push reports to s3, so we add index.html to subfolder urls
# https://github.com/ClickHouse-Extras/woboq_codebrowser/commit/37e15eaf377b920acb0b48dbe82471be9203f76b
RUN git clone https://github.com/ClickHouse-Extras/woboq_codebrowser
RUN cd woboq_codebrowser && cmake . -DCMAKE_BUILD_TYPE=Release && make -j

ENV CODEGEN=/woboq_codebrowser/generator/codebrowser_generator
ENV CODEINDEX=/woboq_codebrowser/indexgenerator/codebrowser_indexgenerator
ENV STATIC_DATA=/woboq_codebrowser/data

ENV SOURCE_DIRECTORY=/repo_folder
ENV BUILD_DIRECTORY=/build
ENV HTML_RESULT_DIRECTORY=$BUILD_DIRECTORY/html_report
ENV SHA=nosha
ENV DATA="data"

CMD mkdir -p $BUILD_DIRECTORY && cd $BUILD_DIRECTORY && \
    cmake $SOURCE_DIRECTORY -DCMAKE_CXX_COMPILER=/usr/bin/clang\+\+-8 -DCMAKE_C_COMPILER=/usr/bin/clang-8 -DCMAKE_EXPORT_COMPILE_COMMANDS=ON && \
    mkdir -p $HTML_RESULT_DIRECTORY && \
    $CODEGEN -b $BUILD_DIRECTORY -a -o $HTML_RESULT_DIRECTORY -p ClickHouse:$SOURCE_DIRECTORY:$SHA -d $DATA && \
    cp -r $STATIC_DATA $HTML_RESULT_DIRECTORY/ &&\
    $CODEINDEX $HTML_RESULT_DIRECTORY -d $DATA && \
    mv $HTML_RESULT_DIRECTORY /test_output
