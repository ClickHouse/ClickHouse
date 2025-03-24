#!/bin/bash
set -ex

S3_PREFIX="s3://io.cribl.cdn.staging/cribl-search/clickhouse"

PROGRAMS_DIR="${BUILD_DIR}/programs"
TARBALL="clickhouse.tgz"
TARBALL_PATH="${WORKSPACE}/${TARBALL}"
COMMIT="$(git rev-parse HEAD)"

# compress the binary and linked files
(
  cd ${PROGRAMS_DIR}
  tar -czf ${TARBALL_PATH} clickhouse*
)

# checksums
(
  cd ${WORKSPACE}
  md5sum ${TARBALL} > ${TARBALL}.md5
  sha256sum ${TARBALL} > ${TARBALL}.sha256
)

# copy to S3
for f in $(ls ${TARBALL}*)
do
  aws s3 cp ${f} ${S3_PREFIX}/${COMMIT}/
done