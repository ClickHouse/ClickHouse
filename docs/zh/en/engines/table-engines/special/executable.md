---
description: '`Executable` 和 `ExecutablePool` 表引擎允许您定义一个表，其行由您定义的脚本生成（通过将行写入 **stdout**）。'
sidebar_label: 'Executable/ExecutablePool'
sidebar_position: 40
slug: /engines/table-engines/special/executable
title: 'Executable 和 ExecutablePool 表引擎'
doc_type: 'reference'
---

`Executable` 和 `ExecutablePool` 表引擎允许您定义一个表，表中的行由您定义的脚本生成 (通过向 **stdout** 写入行) 。可执行脚本存储在 `user_scripts` 目录中，并且可以从任何来源读取数据。

* `Executable` 表：脚本会在每次查询时运行
* `ExecutablePool` 表：维护一个持久化进程池，并在读取时从池中取出进程

您还可以选择包含一个或多个输入查询，将其结果流式传输到 **stdin**，供脚本读取。

<div id="creating-an-executable-table">
  ## 创建 `Executable` 表
</div>

`Executable` 表引擎需要两个参数：脚本名称和输入数据的格式。你也可以选择传入一个或多个输入查询：

```sql
Executable(script_name, format, [input_query...])
```

以下是 `Executable` 表的相关设置：

* `send_chunk_header`
  * 说明：在将每个 chunk 发送给进程处理之前，先发送该 chunk 中的行数。此设置有助于以更高效的方式编写脚本，从而预先分配部分资源
  * 默认值：false
* `command_termination_timeout`
  * 说明：命令终止超时时间，单位为秒
  * 默认值：10
* `command_read_timeout`
  * 说明：从命令 stdout 读取数据的超时时间，单位为毫秒
  * 默认值：10000
* `command_write_timeout`
  * 说明：向命令 stdin 写入数据的超时时间，单位为毫秒
  * 默认值：10000

我们来看一个示例。下面的 Python 脚本名为 `my_script.py`，保存在 `user_scripts` 文件夹中。它读取一个数字 `i`，并输出 `i` 个随机字符串，每个字符串前面都有一个数字，二者之间以制表符分隔：

```python
#!/usr/bin/python3

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

下面的 `my_executable_table` 是基于 `my_script.py` 的输出构建的；每次从 `my_executable_table` 执行 `SELECT` 时，它都会生成 10 个随机字符串：

```sql
CREATE TABLE my_executable_table (
   x UInt32,
   y String
)
ENGINE = Executable('my_script.py', TabSeparated, (SELECT 10))
```

创建该表后会立即返回，不会调用该脚本。查询 `my_executable_table` 时会调用该脚本：

```sql
SELECT * FROM my_executable_table
```

```response
┌─x─┬─y──────────┐
│ 0 │ BsnKBsNGNH │
│ 1 │ mgHfBCUrWM │
│ 2 │ iDQAVhlygr │
│ 3 │ uNGwDuXyCk │
│ 4 │ GcFdQWvoLB │
│ 5 │ UkciuuOTVO │
│ 6 │ HoKeCdHkbs │
│ 7 │ xRvySxqAcR │
│ 8 │ LKbXPHpyDI │
│ 9 │ zxogHTzEVV │
└───┴────────────┘
```

<div id="passing-query-results-to-a-script">
  ## 将查询结果传递给脚本
</div>

Hacker News 网站的用户会发表评论。Python 提供了一个自然语言处理工具包 (`nltk`) ，其中的 `SentimentIntensityAnalyzer` 可用于判断评论是正面、负面还是中性——并为其赋予一个介于 -1 (非常负面的评论) 到 1 (非常正面的评论) 之间的值。让我们创建一个 `Executable` 表，使用 `nltk` 计算 Hacker News 评论的情感倾向。

本示例使用[此处](/zh/engines/table-engines/mergetree-family/textindexes/#hacker-news-dataset)介绍的 `hackernews` 表。`hackernews` 表包含一个 `UInt64` 类型的 `id` 列，以及一个名为 `comment` 的 `String` 列。我们先从定义 `Executable` 表开始：

```sql
CREATE TABLE sentiment (
   id UInt64,
   sentiment Float32
)
ENGINE = Executable(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```

关于 `sentiment` 表的一些说明：

* 文件 `sentiment.py` 保存在 `user_scripts` 文件夹中 (这是 `user_scripts_path` 设置的默认文件夹)
* `TabSeparated` 格式表示我们的 Python 脚本需要生成原始数据行，其中的值以制表符分隔
* 该查询从 `hackernews` 中选择两列。Python 脚本需要从传入的行中解析出这两列的值

下面是 `sentiment.py` 的定义：

```python
#!/usr/local/bin/python3.9

import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def main():
    sentiment_analyzer = SentimentIntensityAnalyzer()

    while True:
        try:
            row = sys.stdin.readline()
            if row == '':
                break

            split_line = row.split("\t")

            id = str(split_line[0])
            comment = split_line[1]

            score = sentiment_analyzer.polarity_scores(comment)['compound']
            print(id + '\t' + str(score) + '\n', end='')
            sys.stdout.flush()
        except BaseException as x:
            break

if __name__ == "__main__":
    main()
```

关于我们的 Python 脚本，有几点说明：

* 要让它正常工作，你需要运行 `nltk.downloader.download('vader_lexicon')`。这一步本来也可以放到脚本里，但那样一来，每次在 `sentiment` 表上执行查询时都会下载一次——效率不高
* `row` 中的每个值都会对应 `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` 结果集中的一行
* 输入的行是以制表符分隔的，因此我们使用 Python 的 `split` 函数解析出 `id` 和 `comment`
* `polarity_scores` 的结果是一个 JSON 对象，其中包含几个值。我们决定只提取这个 JSON 对象中的 `compound` 值
* 别忘了，ClickHouse 中的 `sentiment` 表使用 `TabSeparated` 格式，并且包含两列，因此我们的 `print` 函数会用制表符分隔这两列

每次你编写一个从 `sentiment` 表中选择行的查询时，都会执行 `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` 这个查询，并将结果传递给 `sentiment.py`。让我们来试一下：

```sql
SELECT *
FROM sentiment
```

返回结果如下：

```response
┌───────id─┬─sentiment─┐
│  7398199 │    0.4404 │
│ 21640317 │    0.1779 │
│ 21462000 │         0 │
│ 25168863 │         0 │
│ 25168978 │   -0.1531 │
│ 25169359 │         0 │
│ 25169394 │   -0.9231 │
│ 25169766 │    0.4137 │
│ 25172570 │    0.7469 │
│ 25173687 │    0.6249 │
│ 28291534 │         0 │
│ 28291669 │   -0.4767 │
│ 28291731 │         0 │
│ 28291949 │   -0.4767 │
│ 28292004 │    0.3612 │
│ 28292050 │    -0.296 │
│ 28292322 │         0 │
│ 28295172 │    0.7717 │
│ 28295288 │    0.4404 │
│ 21465723 │   -0.6956 │
└──────────┴───────────┘
```

<div id="creating-an-executablepool-table">
  ## 创建 `ExecutablePool` 表
</div>

`ExecutablePool` 的语法与 `Executable` 类似，但 `ExecutablePool` 表还有几个特有的相关设置：

* `pool_size`
  * 说明：进程池大小。如果该值为 0，则不限制大小
  * 默认值：16
* `max_command_execution_time`
  * 说明：命令的最大执行时间，单位为秒
  * 默认值：10

我们可以很方便地将上面的 `sentiment` 表改为使用 `ExecutablePool`，而不是 `Executable`：

```sql
CREATE TABLE sentiment_pooled (
   id UInt64,
   sentiment Float32
)
ENGINE = ExecutablePool(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20000)
)
SETTINGS
    pool_size = 4;
```

当您的客户端查询 `sentiment_pooled` 表时，ClickHouse 会按需保留 4 个进程。