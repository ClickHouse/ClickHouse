---
toc_priority: 70
toc_title: "Используемые сторонние библиотеки"
---


# Используемые сторонние библиотеки {#ispolzuemye-storonnie-biblioteki}

| Библиотека          | Лицензия                                                                                                                                     |
|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| base64              | [BSD 2-Clause License](https://github.com/aklomp/base64/blob/a27c565d1b6c676beaf297fe503c4518185666f7/LICENSE)                               |
| boost               | [Boost Software License 1.0](https://github.com/ClickHouse-Extras/boost-extra/blob/6883b40449f378019aec792f9983ce3afc7ff16e/LICENSE_1_0.txt) |
| brotli              | [MIT](https://github.com/google/brotli/blob/master/LICENSE)                                                                                  |
| capnproto           | [MIT](https://github.com/capnproto/capnproto/blob/master/LICENSE)                                                                            |
| cctz                | [Apache License 2.0](https://github.com/google/cctz/blob/4f9776a310f4952454636363def82c2bf6641d5f/LICENSE.txt)                               |
| double-conversion   | [BSD 3-Clause License](https://github.com/google/double-conversion/blob/cf2f0f3d547dc73b4612028a155b80536902ba02/LICENSE)                    |
| FastMemcpy          | [MIT](https://github.com/ClickHouse/ClickHouse/blob/master/libs/libmemcpy/impl/LICENSE)                                                      |
| googletest          | [BSD 3-Clause License](https://github.com/google/googletest/blob/master/LICENSE)                                                             |
| h3                  | [Apache License 2.0](https://github.com/uber/h3/blob/master/LICENSE)                                                                         |
| hyperscan           | [BSD 3-Clause License](https://github.com/intel/hyperscan/blob/master/LICENSE)                                                               |
| libcxxabi           | [BSD + MIT](https://github.com/ClickHouse/ClickHouse/blob/master/libs/libglibc-compatibility/libcxxabi/LICENSE.TXT)                          |
| libdivide           | [Zlib License](https://github.com/ClickHouse/ClickHouse/blob/master/contrib/libdivide/LICENSE.txt)                                           |
| libgsasl            | [LGPL v2.1](https://github.com/ClickHouse-Extras/libgsasl/blob/3b8948a4042e34fb00b4fb987535dc9e02e39040/LICENSE)                             |
| libhdfs3            | [Apache License 2.0](https://github.com/ClickHouse-Extras/libhdfs3/blob/bd6505cbb0c130b0db695305b9a38546fa880e5a/LICENSE.txt)                |
| libmetrohash        | [Apache License 2.0](https://github.com/ClickHouse/ClickHouse/blob/master/contrib/libmetrohash/LICENSE)                                      |
| libpcg-random       | [Apache License 2.0](https://github.com/ClickHouse/ClickHouse/blob/master/contrib/libpcg-random/LICENSE-APACHE.txt)                          |
| libressl            | [OpenSSL License](https://github.com/ClickHouse-Extras/ssl/blob/master/COPYING)                                                              |
| librdkafka          | [BSD 2-Clause License](https://github.com/edenhill/librdkafka/blob/363dcad5a23dc29381cc626620e68ae418b3af19/LICENSE)                         |
| libwidechar_width  | [CC0 1.0 Universal](https://github.com/ClickHouse/ClickHouse/blob/master/libs/libwidechar_width/LICENSE)                                     |
| llvm                | [BSD 3-Clause License](https://github.com/ClickHouse-Extras/llvm/blob/163def217817c90fb982a6daf384744d8472b92b/llvm/LICENSE.TXT)             |
| lz4                 | [BSD 2-Clause License](https://github.com/lz4/lz4/blob/c10863b98e1503af90616ae99725ecd120265dfb/LICENSE)                                     |
| mariadb-connector-c | [LGPL v2.1](https://github.com/ClickHouse-Extras/mariadb-connector-c/blob/3.1/COPYING.LIB)                                                   |
| murmurhash          | [Public Domain](https://github.com/ClickHouse/ClickHouse/blob/master/contrib/murmurhash/LICENSE)                                             |
| pdqsort             | [Zlib License](https://github.com/ClickHouse/ClickHouse/blob/master/contrib/pdqsort/license.txt)                                             |
| poco                | [Boost Software License - Version 1.0](https://github.com/ClickHouse-Extras/poco/blob/fe5505e56c27b6ecb0dcbc40c49dc2caf4e9637f/LICENSE)      |
| protobuf            | [BSD 3-Clause License](https://github.com/ClickHouse-Extras/protobuf/blob/12735370922a35f03999afff478e1c6d7aa917a4/LICENSE)                  |
| re2                 | [BSD 3-Clause License](https://github.com/google/re2/blob/7cf8b88e8f70f97fd4926b56aa87e7f53b2717e0/LICENSE)                                  |
| UnixODBC            | [LGPL v2.1](https://github.com/ClickHouse-Extras/UnixODBC/tree/b0ad30f7f6289c12b76f04bfb9d466374bb32168)                                     |
| zlib-ng             | [Zlib License](https://github.com/ClickHouse-Extras/zlib-ng/blob/develop/LICENSE.md)                                                         |
| zstd                | [BSD 3-Clause License](https://github.com/facebook/zstd/blob/dev/LICENSE)                                                                    |

## Рекомендации по добавлению сторонних библиотек и поддержанию в них пользовательских изменений {#adding-third-party-libraries}

1. Весь внешний сторонний код должен находиться в папке `contrib` в отдельных поддиректориях репозитория ClickHouse. По возможности используйте подмодули Git.
2. Клонируйте официальный репозиторий [Clickhouse-extras](https://github.com/ClickHouse-Extras). Используйте официальные репозитории GitHub, если они доступны.
3. Создайте новую ветку на основе той ветки, которую вы хотите интегрировать: например, `master` -> `clickhouse/master`, or `release/vX.Y.Z` -> `clickhouse/release/vX.Y.Z`.
4. Все форки в [Clickhouse-extras](https://github.com/ClickHouse-Extras) может быть автоматически синхронизированы с upstreams. Ветки `clickhouse/...` останутся незатронутыми, поскольку практически никто не будет использовать этот шаблон именования в своих репозиториях выше по потоку.
5. Добавьте подмодули в папку `contrib` репозитория ClickHouse, на который ссылаются вышеуказанные форки/зеркала. Настройте подмодули для отслеживания изменений в соответствующих ветках `clickhouse/...`.
6. Каждый раз, когда необходимо вносить пользовательские изменения в код библиотеки, следует создавать специальную ветвь, например `clickhouse/my-fix`. Затем эта ветка должна быть влита в ветку, отслеживаемую подмодулем, например, `clickhouse/master` или `clickhouse/release/vX.Y.Z`.
7. Не добавляйте код в форки [Clickhouse-extras](https://github.com/ClickHouse-Extras), если имя форка
не соответствует шаблону `clickhouse/...`.
8. Всегда добавляйте пользовательские изменения с учетом официального репозитория. После того как PR будет объединен из (ветки функций/исправлений) вашего личного форка в [Clickhouse-extras](https://github.com/ClickHouse-Extras) и подмодуль будет добавлен в репозиторий ClickHouse, следующие PR из (ветки функций/исправлений) делайте на форке [Clickhouse-extras](https://github.com/ClickHouse-Extras) в официальном репозитории библиотеки.Это позволит убедиться, что: 1) публикуемый код имеет несколько вариант использования и важность; 2) другие пользователи также могут использовать его в своих целях; 3) поддержка кода доступна не только разработчикам ClickHouse.
9. Чтобы подмодуль начал использовать новый код из исходной ветви (например, `master`), сначала следует выполнить тщательное слияние (`master` -> `clickhouse/master`), и только после этого подмодуль можно будет интегрировать в ClickHouse. Это связано с тем, что пользовательские изменения могут быть объединены в отслеживаемой ветке (например, `clickhouse/master`) и поэтому он может отличаться от своего первоначального аналога (`master`).
