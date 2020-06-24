---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: INTO OUTFILE
---

# INTO OUTFILE条款 {#into-outfile-clause}

添加 `INTO OUTFILE filename` 子句（其中filename是字符串文字） `SELECT query` 将其输出重定向到客户端上的指定文件。

## 实施细节 {#implementation-details}

-   此功能是在可用 [命令行客户端](../../../interfaces/cli.md) 和 [ﾂ环板-ｮﾂ嘉ｯﾂ偲](../../../operations/utilities/clickhouse-local.md). 因此，通过发送查询 [HTTP接口](../../../interfaces/http.md) 都会失败
-   如果具有相同文件名的文件已经存在，则查询将失败。
-   默认值 [输出格式](../../../interfaces/formats.md) 是 `TabSeparated` （就像在命令行客户端批处理模式中一样）。
