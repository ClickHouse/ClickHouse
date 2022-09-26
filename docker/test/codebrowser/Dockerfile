# rebuild in #33610
# docker build --network=host -t clickhouse/codebrowser .
# docker run --volume=path_to_repo:/repo_folder --volume=path_to_result:/test_output clickhouse/codebrowser
ARG FROM_TAG=latest
FROM clickhouse/binary-builder:$FROM_TAG

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update && apt-get --yes --allow-unauthenticated install clang-14 libllvm14 libclang-14-dev libmlir-14-dev

# repo versions doesn't work correctly with C++17
# also we push reports to s3, so we add index.html to subfolder urls
# https://github.com/ClickHouse-Extras/woboq_codebrowser/commit/37e15eaf377b920acb0b48dbe82471be9203f76b
# TODO: remove branch in a few weeks after merge, e.g. in May or June 2022
RUN git clone https://github.com/ClickHouse-Extras/woboq_codebrowser --branch llvm-14 \
  && cd woboq_codebrowser \
  && cmake . -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=clang\+\+-14 -DCMAKE_C_COMPILER=clang-14 \
  && make -j \
  && cd .. \
  && rm -rf woboq_codebrowser

ENV CODEGEN=/woboq_codebrowser/generator/codebrowser_generator
ENV CODEINDEX=/woboq_codebrowser/indexgenerator/codebrowser_indexgenerator
ENV STATIC_DATA=/woboq_codebrowser/data

ENV SOURCE_DIRECTORY=/repo_folder
ENV BUILD_DIRECTORY=/build
ENV HTML_RESULT_DIRECTORY=$BUILD_DIRECTORY/html_report
ENV SHA=nosha
ENV DATA="https://s3.amazonaws.com/clickhouse-test-reports/codebrowser/data"

CMD mkdir -p $BUILD_DIRECTORY && cd $BUILD_DIRECTORY && \
    cmake $SOURCE_DIRECTORY -DCMAKE_CXX_COMPILER=/usr/bin/clang\+\+-14 -DCMAKE_C_COMPILER=/usr/bin/clang-14 -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_S3=0 && \
    mkdir -p $HTML_RESULT_DIRECTORY && \
    $CODEGEN -b $BUILD_DIRECTORY -a -o $HTML_RESULT_DIRECTORY -p ClickHouse:$SOURCE_DIRECTORY:$SHA -d $DATA | ts '%Y-%m-%d %H:%M:%S' && \
    cp -r $STATIC_DATA $HTML_RESULT_DIRECTORY/ &&\
    $CODEINDEX $HTML_RESULT_DIRECTORY -d "$DATA" | ts '%Y-%m-%d %H:%M:%S' && \
    mv $HTML_RESULT_DIRECTORY /test_output
