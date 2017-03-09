
## [1.1.54178](https://github.com/yandex/Clickhouse/tree/v1.1.54178-testing) (2017-03-02)
[Full Changelog](https://github.com/yandex/Clickhouse/compare/v1.1.54165-stable...v1.1.54178-testing)

- listen_host by default changed to ::1 and 127.0.0.1.
  If you want use connections from other computers write to config.xml: `<listen_host>::</listen_host>`
