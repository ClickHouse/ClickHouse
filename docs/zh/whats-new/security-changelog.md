---
toc_priority: 76
toc_title: 安全更新日志
---

## 修复于ClickHouse Release 19.14.3.3, 2019-09-10 {#fixed-in-clickhouse-release-19-14-3-3-2019-09-10}

### CVE-2019-15024 {#cve-2019-15024}

对ZooKeeper具有写访问权限并且可以运行ClickHouse所在网络上可用的自定义服务器的攻击者可以创建一个自定义的恶意服务器，该服务器将充当ClickHouse副本并在ZooKeeper中注册。当另一个副本将从恶意副本获取数据部分时，它可以强制clickhouse服务器写入文件系统上的任意路径。

作者：Yandex信息安全团队Eldar Zaitov

### CVE-2019-16535 {#cve-2019-16535}

解压算法中的OOB-read、OOB-write和整数下溢可以通过本机协议实现RCE或DoS。

作者: Yandex信息安全团队Eldar Zaitov

### CVE-2019-16536 {#cve-2019-16536}

恶意的经过身份验证的客户端可能会触发导致DoS的堆栈溢出。

作者: Yandex信息安全团队Eldar Zaitov

## 修复于ClickHouse Release 19.13.6.1, 2019-09-20 {#fixed-in-clickhouse-release-19-13-6-1-2019-09-20}

### CVE-2019-18657 {#cve-2019-18657}

表函数`url`存在允许攻击者在请求中插入任意HTTP标头的漏洞。

作者: [Nikita Tikhomirov](https://github.com/NSTikhomirov)

## 修复于ClickHouse Release 18.12.13, 2018-09-10 {#fixed-in-clickhouse-release-18-12-13-2018-09-10}

### CVE-2018-14672 {#cve-2018-14672}

加载CatBoost模型的函数允许路径遍历和通过错误消息读取任意文件。

作者：Yandex信息安全团队Andrey Krasichkov

## 修复于Release 18.10.3, 2018-08-13 {#fixed-in-clickhouse-release-18-10-3-2018-08-13}

### CVE-2018-14671 {#cve-2018-14671}

unixODBC允许从文件系统加载任意共享对象，从而导致远程代码执行漏洞。

作者：Yandex信息安全团队Andrey Krasichkov和Evgeny Sidorov

## 修复于ClickHouse Release 1.1.54388, 2018-06-28 {#fixed-in-clickhouse-release-1-1-54388-2018-06-28}

### CVE-2018-14668 {#cve-2018-14668}

`remote`表函数允许在`user`，`password`和`default_database`字段中使用任意符号，从而导致跨协议请求伪造攻击。

者：Yandex信息安全团队Andrey Krasichkov

## 修复于ClickHouse Release 1.1.54390, 2018-07-06 {#fixed-in-clickhouse-release-1-1-54390-2018-07-06}

### CVE-2018-14669 {#cve-2018-14669}

ClickHouse MySQL客户端启用了`LOAD DATA LOCAL INFILE`功能，允许恶意MySQL数据库从连接的ClickHouse服务器读取任意文件。

作者：Yandex信息安全团队Andrey Krasichkov和Evgeny Sidorov

## 修复于ClickHouse Release 1.1.54131, 2017-01-10 {#fixed-in-clickhouse-release-1-1-54131-2017-01-10}

### CVE-2018-14670 {#cve-2018-14670}

deb包中的错误配置可能导致未经授权使用数据库。

作者：英国国家网络安全中心（NCSC）

{## [Original article](https://clickhouse.tech/docs/en/security_changelog/) ##}
