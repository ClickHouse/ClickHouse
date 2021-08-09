---
toc_title: INTO OUTFILE
---

# INTO OUTFILE 子句 {#into-outfile-clause}

添加 `INTO OUTFILE filename` 子句（其中filename是字符串） `SELECT query` 将其输出重定向到客户端上的指定文件。

## 实现细节 {#implementation-details}

-   此功能是在可用 [命令行客户端](../../../interfaces/cli.md) 和 [clickhouse-local](../../../operations/utilities/clickhouse-local.md). 因此通过 [HTTP接口](../../../interfaces/http.md) 发送查询将会失败。
-   如果具有相同文件名的文件已经存在，则查询将失败。
-   默认值 [输出格式](../../../interfaces/formats.md) 是 `TabSeparated` （就像在命令行客户端批处理模式中一样）。
