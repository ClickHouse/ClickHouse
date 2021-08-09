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

1. Весь внешний сторонний код должен находиться в отдельных папках внутри папки `contrib` репозитория ClickHouse. По возможности, используйте сабмодули Git.
2. Клонируйте официальный репозиторий [Clickhouse-extras](https://github.com/ClickHouse-Extras). Используйте официальные репозитории GitHub, если они доступны.
3. Создавайте новую ветку на основе той ветки, которую вы хотите интегрировать: например, `master` -> `clickhouse/master` или `release/vX.Y.Z` -> `clickhouse/release/vX.Y.Z`.
4. Все копии [Clickhouse-extras](https://github.com/ClickHouse-Extras) можно автоматически синхронизировать с удаленными репозиториями. Ветки `clickhouse/...` останутся незатронутыми, поскольку скорее всего никто не будет использовать этот шаблон именования в своих репозиториях.
5. Добавьте сабмодули в папку `contrib` репозитория ClickHouse, на который ссылаются клонированные репозитории. Настройте сабмодули для отслеживания изменений в соответствующих ветках `clickhouse/...`.
6. Каждый раз, когда необходимо внести изменения в код библиотеки, следует создавать отдельную ветку, например `clickhouse/my-fix`. Затем эта ветка должна быть слита (`merge`) в ветку, отслеживаемую сабмодулем, например, в `clickhouse/master` или `clickhouse/release/vX.Y.Z`.
7. Не добавляйте код в клоны репозитория [Clickhouse-extras](https://github.com/ClickHouse-Extras), если имя ветки не соответствует шаблону `clickhouse/...`.
8. Всегда вносите изменения с учетом того, что они попадут в официальный репозиторий. После того как PR будет влит из (ветки разработки/исправлений) вашего личного клона репозитория в  [Clickhouse-extras](https://github.com/ClickHouse-Extras), и сабмодуль будет добавлен в репозиторий ClickHouse, рекомендуется сделать еще один PR из (ветки разработки/исправлений) репозитория [Clickhouse-extras](https://github.com/ClickHouse-Extras) в официальный репозиторий библиотеки. Таким образом будут решены следующие задачи: 1) публикуемый код может быть использован многократно и будет иметь более высокую ценность; 2) другие пользователи также смогут использовать его в своих целях; 3) поддержкой кода будут заниматься не только разработчики ClickHouse.
9. Чтобы сабмодуль начал использовать новый код из исходной ветки (например, `master`), сначала следует аккуратно выполнить слияние (`master` -> `clickhouse/master`), и только после этого изменения могут быть добавлены в основной репозиторий ClickHouse. Это связано с тем, что в отслеживаемую ветку (например, `clickhouse/master`) могут быть внесены изменения, и поэтому ветка может отличаться от первоисточника (`master`).
