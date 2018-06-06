# ClickHouse Server Docker Image

## What is ClickHouse?

ClickHouse is an open-source column-oriented database management system that allows generating analytical data reports in real time.

ClickHouse manages extremely large volumes of data in a stable and sustainable manner. It currently powers [Yandex.Metrica](https://metrica.yandex.com/), world’s [second largest](http://w3techs.com/technologies/overview/traffic_analysis/all) web analytics platform, with over 13 trillion database records and over 20 billion events a day, generating customized reports on-the-fly, directly from non-aggregated data. This system was successfully implemented at [CERN’s LHCb experiment](https://www.yandex.com/company/press_center/press_releases/2012/2012-04-10/) to store and process metadata on 10bn events with over 1000 attributes per event registered in 2011.

For more information and documentation see https://clickhouse.yandex/.

## How to use this image

### start server instance
```bash
$ docker run -d --name some-clickhouse-server --ulimit nofile=262144:262144 yandex/clickhouse-server
```

### connect to it from a native client
```bash
$ docker run -it --rm --link some-clickhouse-server:clickhouse-server yandex/clickhouse-client --host clickhouse-server
```

More information about [ClickHouse client](https://clickhouse.yandex/docs/en/interfaces/cli/).

## Configuration

Container exposes 8123 port for [HTTP interface](https://clickhouse.yandex/docs/en/interfaces/http_interface/) and 9000 port for [native client](https://clickhouse.yandex/docs/en/interfaces/tcp/).

ClickHouse configuration represented with a file "config.xml" ([documentation](https://clickhouse.yandex/docs/en/operations/configuration_files/))

### start server instance with custom configuration
```bash
$ docker run -d --name some-clickhouse-server --ulimit nofile=262144:262144 -v /path/to/your/config.xml:/etc/clickhouse-server/config.xml yandex/clickhouse-server
```

## License

View [license information](https://github.com/yandex/ClickHouse/blob/master/LICENSE) for the software contained in this image.
