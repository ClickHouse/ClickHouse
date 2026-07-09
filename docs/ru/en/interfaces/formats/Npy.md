---
alias: []
description: 'Документация по формату Npy'
input_format: true
keywords: ['Npy']
output_format: true
slug: /interfaces/formats/Npy
title: 'Npy'
doc_type: 'reference'
---

| Ввод | Вывод | Алиас |
| ---- | ----- | ----- |
| ✔    | ✔     |       |

<div id="description">
  ## Описание
</div>

Формат `Npy` предназначен для загрузки массива NumPy из файла `.npy` в ClickHouse.
Формат файлов NumPy — это бинарный формат, используемый для эффективного хранения массивов числовых данных.
При импорте ClickHouse интерпретирует измерение верхнего уровня как массив строк с одним столбцом.

В таблице ниже приведены поддерживаемые типы данных Npy и соответствующие им типы в ClickHouse:

<div id="data_types-matching">
  ## Соответствие типов данных
</div>

| Тип данных Npy (`INSERT`) | Тип данных ClickHouse                                   | Тип данных Npy (`SELECT`) |
| ------------------------- | ------------------------------------------------------- | ------------------------- |
| `i1`                      | [Int8](/ru/sql-reference/data-types/int-uint.md)           | `i1`                      |
| `i2`                      | [Int16](/ru/sql-reference/data-types/int-uint.md)          | `i2`                      |
| `i4`                      | [Int32](/ru/sql-reference/data-types/int-uint.md)          | `i4`                      |
| `i8`                      | [Int64](/ru/sql-reference/data-types/int-uint.md)          | `i8`                      |
| `u1`, `b1`                | [UInt8](/ru/sql-reference/data-types/int-uint.md)          | `u1`                      |
| `u2`                      | [UInt16](/ru/sql-reference/data-types/int-uint.md)         | `u2`                      |
| `u4`                      | [UInt32](/ru/sql-reference/data-types/int-uint.md)         | `u4`                      |
| `u8`                      | [UInt64](/ru/sql-reference/data-types/int-uint.md)         | `u8`                      |
| `f2`, `f4`                | [Float32](/ru/sql-reference/data-types/float.md)           | `f4`                      |
| `f8`                      | [Float64](/ru/sql-reference/data-types/float.md)           | `f8`                      |
| `S`, `U`                  | [String](/ru/sql-reference/data-types/string.md)           | `S`                       |
|                           | [FixedString](/ru/sql-reference/data-types/fixedstring.md) | `S`                       |

<div id="example-usage">
  ## Пример использования
</div>

<div id="saving-an-array-in-npy-format-using-python">
  ### Сохранение массива в формате .npy с помощью Python
</div>

```Python
import numpy as np
arr = np.array([[[1],[2],[3]],[[4],[5],[6]]])
np.save('example_array.npy', arr)
```

<div id="reading-a-numpy-file-in-clickhouse">
  ### Чтение файла NumPy в ClickHouse
</div>

```sql title="Query"
SELECT *
FROM file('example_array.npy', Npy)
```

```response title="Response"
┌─array─────────┐
│ [[1],[2],[3]] │
│ [[4],[5],[6]] │
└───────────────┘
```

<div id="selecting-data">
  ### Выборка данных
</div>

Вы можете выбрать данные из таблицы ClickHouse и сохранить их в файл в формате Npy с помощью следующей команды в клиенте ClickHouse:

```bash
$ clickhouse-client --query="SELECT {column} FROM {some_table} FORMAT Npy" > {filename.npy}
```

<div id="format-settings">
  ## Настройки формата
</div>
