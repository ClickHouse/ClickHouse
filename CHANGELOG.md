
## [1.1.54181](https://github.com/yandex/Clickhouse/tree/v1.1.54181-testing) (2017-03-10)
[Full Changelog](https://github.com/yandex/Clickhouse/compare/v1.1.54165-stable...v1.1.54181-testing)

- https server:
  to enable: get/generate keys, uncomment in config.xml:  `<https_port>8443</https_port>` and tune `<openSSL>` section


- listen_host by default changed to ::1 and 127.0.0.1.
  If you want use connections from other computers write to config.xml: `<listen_host>::</listen_host>`

