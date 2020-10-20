REPO_URL="https://repo.yandex.ru/clickhouse/tgz/stable/"
#or https://repo.yandex.ru/clickhouse/tgz/lts/
#or https://repo.yandex.ru/clickhouse/tgz/testing/
VERSION="20.9.3.45"
TGZ_PACKAGES_FOLDER="${BASH_SOURCE%/*}/ClickHouse/PACKAGES"

mkdir -p ${TGZ_PACKAGES_FOLDER}

cd ${TGZ_PACKAGES_FOLDER}
wget "${REPO_URL}clickhouse-client-${VERSION}.tgz"
wget "${REPO_URL}clickhouse-server-${VERSION}.tgz"
wget "${REPO_URL}clickhouse-common-static-${VERSION}.tgz"