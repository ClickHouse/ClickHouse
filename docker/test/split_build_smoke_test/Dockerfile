# rebuild in #33610
# docker build -t altinityinfra/split-build-smoke-test .
ARG FROM_TAG=latest
FROM altinityinfra/binary-builder:$FROM_TAG

COPY run.sh /run.sh
COPY process_split_build_smoke_test_result.py /

CMD /run.sh
