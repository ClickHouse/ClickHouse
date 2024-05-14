# rebuild in #33610
# docker build -t altinityinfra/unit-test .
ARG FROM_TAG=latest
FROM altinityinfra/stateless-test:$FROM_TAG

RUN apt-get install gdb

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
