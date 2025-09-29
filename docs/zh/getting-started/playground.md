---
slug: /zh/getting-started/playground
sidebar_position: 14
sidebar_label: 体验平台
---

# ClickHouse Playground {#clickhouse-playground}

无需搭建服务或集群，[ClickHouse Playground](https://sql.clickhouse.com)允许人们通过执行查询语句立即体验ClickHouse，在Playground中我们提供了一些示例数据集。

你可以使用任意HTTP客户端向Playground提交查询语句，比如[curl](https://curl.haxx.se)或者[wget](https://www.gnu.org/software/wget/)，也可以通过[JDBC](../interfaces/jdbc.md)或者[ODBC](../interfaces/odbc.md)驱动建立连接，更多信息详见[客户端](../interfaces/index.md)。

## 使用凭证 {#credentials}
  
| 参数                | 值                                  |
|:--------------------|:-----------------------------------|
| HTTPS连接地址        | `https://play.clickhouse.com:443/` |
| 原生TCP连接地址      | `play.clickhouse.com:9440`         |
| 用户名              | `explorer`或者`play`                |
| 密码                | (空)                                |

## 限制 {#limitations}

所有查询语句都会视为一个具有只读权限用户的操作，这意味着存在如下一些限制：

- 不允许执行DDL语句
- 不允许执行INSERT语句

此外，Playground服务对资源使用也有限制。

## 示例 {#examples}

使用`curl`命令：

``` bash
curl "https://play.clickhouse.com/?user=explorer" --data-binary "SELECT 'Play ClickHouse'"
```

使用[命令行客户端](../interfaces/cli.md)：

``` bash
clickhouse client --secure --host play.clickhouse.com --user explorer
```
