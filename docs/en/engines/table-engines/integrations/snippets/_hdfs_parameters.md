| Parameter | Description |
|-----------|-------------|
| `URI` | Whole file URI in HDFS. The path part of `URI` may contain globs. In this case the table would be readonly. |
| `format` | Specifies one of the available file formats. To perform `SELECT` queries, the format must be supported for input, and to perform `INSERT` queries – for output. The available formats are listed in the [Formats](/sql-reference/formats#formats-overview) section. |
| `[PARTITION BY expr]` | Optional partitioning expression. |