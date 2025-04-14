---
slug: /zh/whats-new/security-changelog
sidebar_position: 76
sidebar_label: 安全更新日志
---
# 安全更新日志
## 修复于ClickHouse 22.9.1.2603, 2022-09-22
### CVE-2022-44011
ClickHouse server中发现了一个堆缓冲区溢出问题。拥有向ClickHouse Server导入数据能力的恶意用户，可通过插入畸形CapnProto对象使ClickHouse Server对象崩溃。

修复已推送至版本22.9.1.2603, 22.8.2.11，22.7.4.16，22.6.6.16，22.3.12.19

作者：Kiojj(独立研究者)

### CVE-2022-44010
ClickHouse server中发现了一个堆缓冲区溢出问题。攻击者可发送一个特殊的HTTP请求至HTTP端口（默认监听在8123端口），该攻击可造成堆缓冲区溢出进而使ClickHouse server进程崩溃。执行该攻击无需认证。

修复版本已推送至版本22.9.1.2603，22.8.2.11，22.7.4.16，22.6.6.16，22.3.12.19

作者：Kiojj(独立研究者)

## 修复于ClickHouse 21.10.2.15，2021-10-18
### CVE-2021-43304
在对恶意查询做语法分析时，ClickHouse的LZ4压缩编码会堆缓冲区溢出。LZ4:decompressImpl循环尤其是`wildCopy<copy_amount>(op, ip, copy_end)`中的随意复制操作没有验证是否会导致超出目标缓冲区限制。

作者：JFrog 安全研究团队

### CVE-2021-43305
在对恶意查询做语法分析时，ClickHouse的LZ4压缩编码会堆缓冲区溢出。LZ4:decompressImpl循环尤其是`wildCopy<copy_amount>(op, ip, copy_end)`中的随意复制操作没有验证是否会导致超出目标缓冲区限制。
该问题于CVE-2021-43304非常相似，但是无保护的copy操作存在于不同的wildCopy调用里。

作者：JFrog 安全研究团队

### CVE-2021-42387
在对恶意查询做语法分析时，ClickHouse的LZ4:decompressImpl循环会从压缩数据中读取一个用户提供的16bit无符号值（'offset'）。这个offset后面在复制操作作为长度使用时，没有检查是否超过复制源的上限。

作者：JFrog 安全研究团队

### CVE-2021-42388
在对恶意查询做语法分析时，ClickHouse的LZ4:decompressImpl循环会从压缩数据中读取一个用户提供的16bit无符号值（'offset'）。这个offset后面在复制操作作为长度使用时，没有检查是否越过复制源的下限。

作者：JFrog 安全研究团队

### CVE-2021-42389
在对恶意查询做语法分析时，ClickHouse的Delta压缩编码存在除零错误。压缩缓存的首字节在取模时没有判断是否为0。

作者：JFrog 安全研究团队

### CVE-2021-42390
在对恶意查询做语法分析时，ClickHouse的DeltaDouble压缩编码存在除零错误。压缩缓存的首字节在取模时没有判断是否为0。

作者：JFrog 安全研究团队

### CVE-2021-42391
在对恶意查询做语法分析时，  ClickHouse的Gorilla压缩编码存在除零错误，压缩缓存的首字节取模时没有判断是否为0。

作者：JFrog 安全研究团队

## 修复于ClickHouse 21.4.3.21，2021-04-12
### CVE-2021-25263
拥有CREATE DICTIONARY权限的攻击者，可以读取许可目录之外的任意文件。

修复已推送至版本20.8.18.32-lts，21.1.9.41-stable，21.2.9.41-stable，21.3.6.55-lts，21.4.3.21-stable以及更早期的版本。

作者：[Vyacheslav Egoshin](https://twitter.com/vegoshin)

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

[Original article](/docs/en/whats-new/security-changelog)
