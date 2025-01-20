# rebuild in #33610
# docker build -t altinityinfra/unit-test .
ARG FROM_TAG=latest
FROM altinityinfra/test-base:$FROM_TAG

COPY run.sh /
RUN chmod +x run.sh
ENTRYPOINT ["/run.sh"]
