## 修复于 ClickHouse Release 18.12.13, 2018-09-10 {#xiu-fu-yu-clickhouse-release-18-12-13-2018-09-10}

### CVE-2018-14672 {#cve-2018-14672}

加载CatBoost模型的功能，允许遍历路径并通过错误消息读取任意文件。

来源: Yandex信息安全团队的Andrey Krasichkov

## 修复于 ClickHouse Release 18.10.3, 2018-08-13 {#xiu-fu-yu-clickhouse-release-18-10-3-2018-08-13}

### CVE-2018-14671 {#cve-2018-14671}

unixODBC允许从文件系统加载任意共享对象，从而导致«远程执行代码»漏洞。

来源：Yandex信息安全团队的Andrey Krasichkov和Evgeny Sidorov

## 修复于 ClickHouse Release 1.1.54388, 2018-06-28 {#xiu-fu-yu-clickhouse-release-1-1-54388-2018-06-28}

### CVE-2018-14668 {#cve-2018-14668}

远程表函数功能允许在 «user», «password» 及 «default_database» 字段中使用任意符号，从而导致跨协议请求伪造攻击。

来源：Yandex信息安全团队的Andrey Krasichkov

## 修复于 ClickHouse Release 1.1.54390, 2018-07-06 {#xiu-fu-yu-clickhouse-release-1-1-54390-2018-07-06}

### CVE-2018-14669 {#cve-2018-14669}

ClickHouse MySQL客户端启用了 «LOAD DATA LOCAL INFILE» 功能，该功能允许恶意MySQL数据库从连接的ClickHouse服务器读取任意文件。

来源：Yandex信息安全团队的Andrey Krasichkov和Evgeny Sidorov

## 修复于 ClickHouse Release 1.1.54131, 2017-01-10 {#xiu-fu-yu-clickhouse-release-1-1-54131-2017-01-10}

### CVE-2018-14670 {#cve-2018-14670}

deb软件包中的错误配置可能导致使用未经授权的数据库。

来源：英国国家网络安全中心（NCSC）

[来源文章](https://clickhouse.tech/docs/en/security_changelog/) <!--hide-->
