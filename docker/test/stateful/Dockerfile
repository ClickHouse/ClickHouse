# rebuild in #33610
# docker build -t clickhouse/stateful-test .
ARG FROM_TAG=latest
# TODO consider replacing clickhouse with altinityinfra dockerhub account
FROM altinityinfra/stateless-test:$FROM_TAG

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
        llvm-9 \
        rpm2cpio \
        cpio

COPY s3downloader /s3downloader

ENV S3_URL="https://clickhouse-datasets.s3.amazonaws.com"
ENV DATASETS="hits visits"
ENV EXPORT_S3_STORAGE_POLICIES=1

# Download Minio-related binaries
RUN arch=${TARGETARCH:-amd64} \
    && wget "https://dl.min.io/server/minio/release/linux-${arch}/archive/minio-20220103182258.0.0.x86_64.rpm" \
    && wget "https://dl.min.io/client/mc/release/linux-${arch}/mc" \
    && chmod +x ./mc
ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"
COPY setup_minio.sh /

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
