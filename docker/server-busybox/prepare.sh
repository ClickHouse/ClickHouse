DOCKER_ROOT_FOLDER="${BASH_SOURCE%/*}/root"
TGZ_PACKAGES_FOLDER="${BASH_SOURCE%/*}/ClickHouse/PACKAGES"

# unpack tars
tar xvzf ${TGZ_PACKAGES_FOLDER}/clickhouse-common-static-2*.tgz --strip-components=2 -C "$DOCKER_ROOT_FOLDER"
tar xvzf ${TGZ_PACKAGES_FOLDER}/clickhouse-server-*.tgz --strip-components=2 -C "$DOCKER_ROOT_FOLDER"
tar xvzf ${TGZ_PACKAGES_FOLDER}/clickhouse-client-*.tgz --strip-components=2 -C "$DOCKER_ROOT_FOLDER"

# prepare few more folders
mkdir -p "${DOCKER_ROOT_FOLDER}/etc/clickhouse-server/users.d" \
         "${DOCKER_ROOT_FOLDER}/etc/clickhouse-server/config.d" \
         "${DOCKER_ROOT_FOLDER}/var/log/clickhouse-server" \
         "${DOCKER_ROOT_FOLDER}/var/lib/clickhouse" \
         "${DOCKER_ROOT_FOLDER}/docker-entrypoint-initdb.d" \
         "${DOCKER_ROOT_FOLDER}/lib64"

# cp docker/server/docker_related_config.xml "${DOCKER_ROOT_FOLDER}/etc/clickhouse-server/config.d/"
# cp docker/server/entrypoint.sh "${DOCKER_ROOT_FOLDER}/" <-- it differs, original one uses several bash features, non avaliable on sh

## get glibc components from ubuntu 20.04 and put them to expected place
docker pull ubuntu:20.04
ubuntu20image=$(docker create --rm ubuntu:20.04)
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libc.so.6       "${DOCKER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libdl.so.2      "${DOCKER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libm.so.6       "${DOCKER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libpthread.so.0 "${DOCKER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/librt.so.1      "${DOCKER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libnss_dns.so.2 "${DOCKER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib/x86_64-linux-gnu/libresolv.so.2  "${DOCKER_ROOT_FOLDER}/lib"
docker cp -L ${ubuntu20image}:/lib64/ld-linux-x86-64.so.2           "${DOCKER_ROOT_FOLDER}/lib64"