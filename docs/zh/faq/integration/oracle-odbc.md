---
slug: /zh/faq/integration/oracle-odbc
title: 使用 Oracle ODBC 时遇到编码问题怎么办？
toc_hidden: true
sidebar_position: 20
---

# 使用 Oracle ODBC 时遇到编码问题怎么办？ {#oracle-odbc-encodings}

如果您使用 Oracle 作为 ClickHouse 外部字典的数据源，并通过 Oracle ODBC 驱动程序，您需要在 `/etc/default/clickhouse` 中为 `NLS_LANG` 环境变量设置正确的值。更多信息，请参阅 [Oracle NLS_LANG FAQ](https://www.oracle.com/technetwork/products/globalization/nls-lang-099431.html)。

**示例**

``` sql
NLS_LANG=RUSSIAN_RUSSIA.UTF8
```