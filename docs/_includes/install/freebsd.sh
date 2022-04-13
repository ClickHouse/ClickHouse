fetch 'https://builds.clickhouse.com/master/freebsd/clickhouse'
chmod a+x ./clickhouse
su -m root -c './clickhouse install'
