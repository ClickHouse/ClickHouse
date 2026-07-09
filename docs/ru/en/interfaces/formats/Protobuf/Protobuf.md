---
alias: []
description: 'Документация по формату Protobuf'
input_format: true
keywords: ['Protobuf']
output_format: true
slug: /interfaces/formats/Protobuf
title: 'Protobuf'
doc_type: 'guide'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✔    | ✔     |           |

<div id="description">
  ## Описание
</div>

Формат `Protobuf` — это формат [Protocol Buffers](https://protobuf.dev/).

Для этого формата требуется внешняя схема, которая кэшируется между запросами.

ClickHouse поддерживает:

* синтаксис `proto2` и `proto3`.
* поля `Repeated`/`optional`/`required`.

Чтобы определить соответствие между столбцами таблицы и полями типа сообщения Protocol Buffers, ClickHouse сравнивает их имена.
Это сравнение выполняется без учёта регистра, а символы `_` (подчёркивание) и `.` (точка) считаются одинаковыми.
Если типы столбца и поля сообщения Protocol Buffers различаются, применяется необходимое преобразование.

Поддерживаются вложенные сообщения. Например, для поля `z` в следующем типе сообщения:

```capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

ClickHouse пытается найти столбец с именем `x.y.z` (или `x_y_z`, или `X.y_Z`, и так далее).

Вложенные сообщения подходят для ввода и вывода [вложенных структур данных](/ru/sql-reference/data-types/nested-data-structures/index.md).

Значения по умолчанию, определённые в схеме Protobuf, как в примере ниже, не применяются; вместо них используются [значения по умолчанию таблицы](/ru/sql-reference/statements/create/table#default_values):

```capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

Если сообщение содержит [oneof](https://protobuf.dev/programming-guides/proto3/#oneof) и задан параметр `input_format_protobuf_oneof_presence`, ClickHouse заполняет столбец, который показывает, какое поле в oneof было обнаружено.

```capnp
syntax = "proto3";

message StringOrString {
  oneof string_oneof {
    string string1 = 1;
    string string2 = 42;
  }
}
```

```sql
CREATE TABLE string_or_string ( string1 String, string2 String, string_oneof Enum('no'=0, 'hello' = 1, 'world' = 42))  Engine=MergeTree ORDER BY tuple();
INSERT INTO string_or_string from INFILE '$CURDIR/data_protobuf/String1' SETTINGS format_schema='$SCHEMADIR/string_or_string.proto:StringOrString' FORMAT ProtobufSingle;
SELECT * FROM string_or_string
```

```text
   ┌─────────┬─────────┬──────────────┐
   │ string1 │ string2 │ string_oneof │
   ├─────────┼─────────┼──────────────┤
1. │         │ string2 │ world        │
   ├─────────┼─────────┼──────────────┤
2. │ string1 │         │ hello        │
   └─────────┴─────────┴──────────────┘
```

Имя столбца, указывающего на наличие значения, должно совпадать с именем `oneof`.
Вложенные сообщения поддерживаются (см. [basic-examples](#basic-examples)). Пустые сообщения также поддерживаются.
Допустимые типы: Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64, UInt64, Enum, Enum8 или Enum16.
Enum (а также Enum8 или Enum16) должен содержать все возможные теги `oneof`, а также 0 для обозначения отсутствия; строковые представления значения не имеют.

Настройка [`input_format_protobuf_oneof_presence`](/ru/operations/settings/settings-formats.md#input_format_protobuf_oneof_presence) по умолчанию отключена

ClickHouse принимает и выводит protobuf-сообщения в формате `с префиксом длины`.
Это означает, что перед каждым сообщением должна записываться его длина в виде [целого числа переменной длины (varint)](https://developers.google.com/protocol-buffers/docs/encoding#varints).

<div id="example-usage">
  ## Пример использования
</div>

<div id="basic-examples">
  ### Чтение и запись данных
</div>

:::note Файлы примера
Файлы, используемые в этом примере, доступны в [репозитории с примерами](https://github.com/ClickHouse/formats/ProtoBuf)
:::

В этом примере мы прочитаем данные из файла `protobuf_message.bin` в таблицу ClickHouse. Затем мы запишем их
обратно в файл `protobuf_message_from_clickhouse.bin`, используя формат `Protobuf`.

Пусть дан файл `schemafile.proto`:

```capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

<details>
  <summary>Генерация двоичного файла</summary>

  Если вы уже знаете, как сериализовать и десериализовать данные в формате `Protobuf`, можете пропустить этот шаг.

  Мы будем использовать Python, чтобы сериализовать данные в `protobuf_message.bin` и прочитать их в ClickHouse.
  Если вы хотите использовать другой язык, см. также: [&quot;Как читать и записывать Protobuf-сообщения с префиксом длины на популярных языках&quot;](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

  Выполните следующую команду, чтобы сгенерировать файл Python с именем `schemafile_pb2.py` в
  том же каталоге, что и `schemafile.proto`. Этот файл содержит классы Python,
  которые представляют ваше Protobuf-сообщение `UserData`:

  ```bash
  protoc --python_out=. schemafile.proto
  ```

  Теперь создайте новый файл Python с именем `generate_protobuf_data.py` в том же
  каталоге, что и `schemafile_pb2.py`. Вставьте в него следующий код:

  ```python
  import schemafile_pb2  # Модуль, сгенерированный 'protoc'
  from google.protobuf import text_format
  from google.protobuf.internal.encoder import _VarintBytes # Импорт внутреннего кодировщика varint

  def create_user_data_message(name, surname, birthDate, phoneNumbers):
      """
      Создаёт и заполняет Protobuf-сообщение UserData.
      """
      message = schemafile_pb2.MessageType()
      message.name = name
      message.surname = surname
      message.birthDate = birthDate
      message.phoneNumbers.extend(phoneNumbers)
      return message

  # Данные для пользователей в этом примере
  data_to_serialize = [
      {"name": "Aisha", "surname": "Khan", "birthDate": 19920815, "phoneNumbers": ["(555) 247-8903", "(555) 612-3457"]},
      {"name": "Javier", "surname": "Rodriguez", "birthDate": 20001015, "phoneNumbers": ["(555) 891-2046", "(555) 738-5129"]},
      {"name": "Mei", "surname": "Ling", "birthDate": 19980616, "phoneNumbers": ["(555) 956-1834", "(555) 403-7682"]},
  ]

  output_filename = "protobuf_messages.bin"

  # Открываем двоичный файл в режиме записи ('wb')
  with open(output_filename, "wb") as f:
      for item in data_to_serialize:
          # Создаём экземпляр Protobuf-сообщения для текущего пользователя
          message = create_user_data_message(
              item["name"],
              item["surname"],
              item["birthDate"],
              item["phoneNumbers"]
          )

          # Сериализуем сообщение
          serialized_data = message.SerializeToString()

          # Получаем длину сериализованных данных
          message_length = len(serialized_data)

          # Используем внутреннюю функцию _VarintBytes из библиотеки Protobuf для кодирования длины
          length_prefix = _VarintBytes(message_length)

          # Записываем префикс длины
          f.write(length_prefix)
          # Записываем сериализованные данные сообщения
          f.write(serialized_data)

  print(f"Protobuf messages (length-delimited) written to {output_filename}")

  # --- Необязательно: проверка (чтение и вывод) ---
  # Для чтения мы также используем внутренний декодер varint из Protobuf.
  from google.protobuf.internal.decoder import _DecodeVarint32

  print("\n--- Проверка чтением ---")
  with open(output_filename, "rb") as f:
      buf = f.read() # Считываем весь файл в буфер для более удобного декодирования varint
      n = 0
      while n < len(buf):
          # Декодируем префикс длины varint
          msg_len, new_pos = _DecodeVarint32(buf, n)
          n = new_pos

          # Извлекаем данные сообщения
          message_data = buf[n:n+msg_len]
          n += msg_len

          # Разбираем сообщение
          decoded_message = schemafile_pb2.MessageType()
          decoded_message.ParseFromString(message_data)
          print(text_format.MessageToString(decoded_message, as_utf8=True))
  ```

  Теперь запустите скрипт из командной строки. Рекомендуется запускать его в
  виртуальном окружении Python, например с помощью `uv`:

  ```bash
  uv venv proto-venv
  source proto-venv/bin/activate
  ```

  Вам потребуется установить следующие библиотеки Python:

  ```bash
  uv pip install --upgrade protobuf
  ```

  Запустите скрипт, чтобы сгенерировать двоичный файл:

  ```bash
  python generate_protobuf_data.py
  ```
</details>

Создайте таблицу ClickHouse, соответствующую схеме:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

Вставьте данные в таблицу через командную строку:

```bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.protobuf_messages SETTINGS format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Вы также можете записать данные обратно в двоичный файл в формате `Protobuf`:

```sql
SELECT * FROM test.protobuf_messages INTO OUTFILE 'protobuf_message_from_clickhouse.bin' FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

Имея схему Protobuf, теперь вы можете десериализовать данные, записанные ClickHouse в файл `protobuf_message_from_clickhouse.bin`.

<div id="basic-examples-cloud">
  ### Чтение и запись данных с помощью ClickHouse Cloud
</div>

В ClickHouse Cloud нельзя загрузить файл схемы Protobuf. Однако можно использовать настройку `format_protobuf_schema`,
чтобы указать схему в запросе. В этом примере мы покажем, как прочитать сериализованные данные с локальной
машины и вставить их в таблицу в ClickHouse Cloud.

Как и в предыдущем примере, создайте таблицу в ClickHouse Cloud в соответствии со схемой Protobuf:

```sql
CREATE DATABASE IF NOT EXISTS test;
CREATE TABLE IF NOT EXISTS test.protobuf_messages (
  name String,
  surname String,
  birthDate UInt32,
  phoneNumbers Array(String)
)
ENGINE = MergeTree()
ORDER BY tuple()
```

Параметр `format_schema_source` задаёт источник для параметра `format_schema`

Возможные значения:

* &#39;file&#39; (по умолчанию): не поддерживается в Cloud
* &#39;string&#39;: `format_schema` содержит буквальное содержимое схемы.
* &#39;query&#39;: `format_schema` — это запрос для получения схемы.

<div id="format-schema-source-string">
  ### `format_schema_source='string'`
</div>

Чтобы вставить данные в ClickHouse Cloud, указав схему в виде строки, выполните:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Выберите данные, вставленные в таблицу:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="format-schema-source-query">
  ### `format_schema_source='query'`
</div>

Схему Protobuf также можно хранить в таблице.

Создайте таблицу в ClickHouse Cloud, в которую будут вставляться данные:

```sql
CREATE TABLE testing.protobuf_schema (
  schema String
)
ENGINE = MergeTree()
ORDER BY tuple();
```

```sql
INSERT INTO testing.protobuf_schema VALUES ('syntax = "proto3";message MessageType {  string name = 1;  string surname = 2;  uint32 birthDate = 3;  repeated string phoneNumbers = 4;};');
```

Вставьте данные в ClickHouse Cloud, указав схему в запросе, который нужно выполнить:

```bash
cat protobuf_messages.bin | clickhouse client --host <hostname> --secure --password <password> --query "INSERT INTO testing.protobuf_messages SETTINGS format_schema_source='SELECT schema FROM testing.protobuf_schema', format_schema='schemafile:MessageType' FORMAT Protobuf"
```

Выберите данные, вставленные в таблицу:

```sql
clickhouse client --host <hostname> --secure --password <password> --query "SELECT * FROM testing.protobuf_messages"
```

```response
Aisha Khan 19920815 ['(555) 247-8903','(555) 612-3457']
Javier Rodriguez 20001015 ['(555) 891-2046','(555) 738-5129']
Mei Ling 19980616 ['(555) 956-1834','(555) 403-7682']
```

<div id="using-autogenerated-protobuf-schema">
  ### Использование автоматически сгенерированной схемы
</div>

Если у вас нет внешней схемы Protobuf для ваших данных, вы всё равно можете выводить и вводить данные в формате Protobuf
с помощью автоматически сгенерированной схемы. Для этого используйте настройку `format_protobuf_use_autogenerated_schema`.

Например:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1
```

В этом случае ClickHouse автоматически сгенерирует схему Protobuf в соответствии со структурой таблицы с помощью функции
[`structureToProtobufSchema`](/ru/sql-reference/functions/other-functions#structureToProtobufSchema). Затем эта схема будет использоваться для сериализации данных в формате Protobuf.

Вы также можете читать файл Protobuf с автоматически сгенерированной схемой. В этом случае необходимо, чтобы файл был создан с использованием той же схемы:

```bash
$ cat hits.bin | clickhouse-client --query "INSERT INTO test.hits SETTINGS format_protobuf_use_autogenerated_schema=1 FORMAT Protobuf"
```

Параметр [`format_protobuf_use_autogenerated_schema`](/ru/operations/settings/settings-formats.md#format_protobuf_use_autogenerated_schema) по умолчанию включен и применяется, если [`format_schema`](/ru/operations/settings/formats#format_schema) не задан.

Вы также можете сохранять автоматически сгенерированную схему в файл при вводе/выводе с помощью параметра [`output_format_schema`](/ru/operations/settings/formats#output_format_schema). Например:

```sql
SELECT * FROM test.hits format Protobuf SETTINGS format_protobuf_use_autogenerated_schema=1, output_format_schema='path/to/schema/schema.proto'
```

В этом случае автоматически сгенерированная схема Protobuf будет сохранена в файл `path/to/schema/schema.capnp`.

<div id="drop-protobuf-cache">
  ### Сброс кэша Protobuf
</div>

Чтобы перезагрузить схему Protobuf, загруженную из [`format_schema_path`](/ru/operations/server-configuration-parameters/settings.md/#format_schema_path), используйте команду [`SYSTEM DROP ... FORMAT CACHE`](/ru/sql-reference/statements/system.md/#system-drop-schema-format).

```sql
SYSTEM DROP FORMAT SCHEMA CACHE FOR Protobuf
```