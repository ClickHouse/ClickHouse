---
description: 'На этой странице объясняется, как настроить ClickHouse server с помощью
  файлов конфигурации в формате XML или YAML.'
sidebar_label: 'Файлы конфигурации'
sidebar_position: 50
slug: /operations/configuration-files
title: 'Файлы конфигурации'
doc_type: 'guide'
---

:::note
Профили настроек и файлы конфигурации на основе XML не поддерживаются в ClickHouse Cloud. Поэтому в ClickHouse Cloud вы не найдете файл config.xml. Вместо этого для управления настройками через профили настроек следует использовать SQL-команды.

Подробнее см. в разделе [&quot;Настройка параметров&quot;](/ru/manage/settings)
:::

ClickHouse server можно настроить с помощью файлов конфигурации в формате XML или YAML.
В большинстве вариантов установки ClickHouse server использует `/etc/clickhouse-server/config.xml` в качестве файла конфигурации по умолчанию, но расположение файла конфигурации также можно указать вручную при запуске сервера с помощью параметра командной строки `--config-file` или `-C`.
Дополнительные файлы конфигурации можно размещать в каталоге `config.d/` относительно основного файла конфигурации, например в каталоге `/etc/clickhouse-server/config.d/`.
Файлы в этом каталоге и основная конфигурация объединяются на этапе предварительной обработки перед применением конфигурации в ClickHouse server.
Файлы конфигурации объединяются в алфавитном порядке.
Чтобы упростить обновления и повысить модульность, рекомендуется не изменять файл `config.xml` по умолчанию, а помещать дополнительную пользовательскую конфигурацию в `config.d/`.
Конфигурация ClickHouse Keeper находится в `/etc/clickhouse-keeper/keeper_config.xml`.
Аналогично дополнительные файлы конфигурации для Keeper нужно размещать в `/etc/clickhouse-keeper/keeper_config.d/`.

Можно комбинировать файлы конфигурации XML и YAML, например у вас может быть основной файл конфигурации `config.xml` и дополнительные файлы конфигурации `config.d/network.xml`, `config.d/timezone.yaml` и `config.d/keeper.yaml`.
Смешивание XML и YAML в пределах одного файла конфигурации не поддерживается.
В файлах конфигурации XML в качестве тега верхнего уровня следует использовать `<clickhouse>...</clickhouse>`.
В файлах конфигурации YAML `clickhouse:` указывать необязательно; если он отсутствует, парсер добавляет его автоматически.

<div id="merging">
  ## Слияние конфигурации
</div>

Два файла конфигурации (обычно основной файл конфигурации и ещё один файл конфигурации из `config.d/`) объединяются следующим образом:

* Если узел (то есть путь, ведущий к элементу) присутствует в обоих файлах и не имеет атрибутов `replace` или `remove`, он включается в итоговый файл конфигурации, а дочерние элементы обоих узлов также включаются и рекурсивно объединяются.
* Если один из двух узлов содержит атрибут `replace`, он включается в итоговый файл конфигурации, но в него попадают только дочерние элементы узла с атрибутом `replace`.
* Если один из двух узлов содержит атрибут `remove`, этот узел не включается в итоговый файл конфигурации (а если он уже существует, то удаляется).

Например, даны два файла конфигурации:

```xml title="config.xml"
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
    </config_a>
    <config_b>
        <setting_2>2</setting_2>
    </config_b>
    <config_c>
        <setting_3>3</setting_3>
    </config_c>
</clickhouse>
```

и

```xml title="config.d/other_config.xml"
<clickhouse>
    <config_a>
        <setting_4>4</setting_4>
    </config_a>
    <config_b replace="replace">
        <setting_5>5</setting_5>
    </config_b>
    <config_c remove="remove">
        <setting_6>6</setting_6>
    </config_c>
</clickhouse>
```

Итоговый объединённый конфигурационный файл будет выглядеть так:

```xml
<clickhouse>
    <config_a>
        <setting_1>1</setting_1>
        <setting_4>4</setting_4>
    </config_a>
    <config_b>
        <setting_5>5</setting_5>
    </config_b>
</clickhouse>
```

<div id="from_env_zk">
  ### Подстановка с помощью переменных окружения и узлов ZooKeeper
</div>

Чтобы указать, что значение элемента должно быть заменено значением переменной окружения, можно использовать атрибут `from_env`.

Например, если переменная окружения `$MAX_QUERY_SIZE = 150000`:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size from_env="MAX_QUERY_SIZE"/>
        </default>
    </profiles>
</clickhouse>
```

В результате получится следующая конфигурация:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

То же самое можно сделать с помощью `from_zk` (узла ZooKeeper):

```xml
<clickhouse>
    <postgresql_port from_zk="/zk_configs/postgresql_port"/>
</clickhouse>
```

```shell
# clickhouse-keeper-client
/ :) touch /zk_configs
/ :) create /zk_configs/postgresql_port "9005"
/ :) get /zk_configs/postgresql_port
9005
```

В результате получаем следующую конфигурацию:

```xml
<clickhouse>
    <postgresql_port>9005</postgresql_port>
</clickhouse>
```

<div id="default-values">
  #### Значения по умолчанию
</div>

Элемент с атрибутом `from_env` или `from_zk` может также иметь атрибут `replace="1"` (последний должен располагаться перед `from_env`/`from_zk`).
В этом случае для элемента можно задать значение по умолчанию.
Элемент принимает значение переменной окружения или узла ZooKeeper, если оно задано; в противном случае используется значение по умолчанию.

Ниже повторяется предыдущий пример, но при условии, что `MAX_QUERY_SIZE` не задан:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size replace="1" from_env="MAX_QUERY_SIZE">150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

В результате получаем конфигурацию:

```xml
<clickhouse>
    <profiles>
        <default>
            <max_query_size>150000</max_query_size>
        </default>
    </profiles>
</clickhouse>
```

<div id="substitution-with-file-content">
  ## Подстановка содержимого файла
</div>

Части конфигурации также можно заменять содержимым файлов. Это можно сделать двумя способами:

* *Подстановка значений*: Если у элемента есть атрибут `incl`, его значение будет заменено содержимым указанного файла. По умолчанию путь к файлу с подстановками — `/etc/metrika.xml`. Это можно изменить в элементе [`include_from`](../operations/server-configuration-parameters/settings.md#include_from) в конфигурации сервера. Значения подстановок задаются в элементах `/clickhouse/substitution_name` этого файла. Если подстановка, указанная в `incl`, не существует, это записывается в журнал. Чтобы ClickHouse не записывал отсутствующие подстановки в журнал, укажите атрибут `optional="true"` (например, для настроек [macros](../operations/server-configuration-parameters/settings.md#macros)).
* *Подстановка элементов*: Если вы хотите заменить подстановкой весь элемент, используйте `include` в качестве имени элемента. Имя элемента `include` можно использовать вместе с атрибутом `from_zk = "/path/to/node"`. В этом случае значение элемента заменяется содержимым узла ZooKeeper по пути `/path/to/node`. Это также работает, если вы храните в узле ZooKeeper целое поддерево XML: оно будет полностью вставлено в исходный элемент.

Пример показан ниже:

```xml
<clickhouse>
    <!-- Appends XML subtree found at `/profiles-in-zookeeper` ZK path to `<profiles>` element. -->
    <profiles from_zk="/profiles-in-zookeeper" />

    <users>
        <!-- Replaces `include` element with the subtree found at `/users-in-zookeeper` ZK path. -->
        <include from_zk="/users-in-zookeeper" />
        <include from_zk="/other-users-in-zookeeper" />
    </users>
</clickhouse>
```

Если вы хотите объединить подставляемое содержимое с существующей конфигурацией вместо добавления, можно использовать атрибут `merge="true"`. Например: `<include from_zk="/some_path" merge="true">`. В этом случае существующая конфигурация будет объединена с содержимым из подстановки, а существующие параметры конфигурации будут заменены значениями из подстановки.

<div id="encryption">
  ## Шифрование и скрытие конфигурации
</div>

Вы можете использовать симметричное шифрование, чтобы зашифровать элемент конфигурации, например пароль в открытом виде или закрытый ключ.
Для этого сначала настройте [кодек шифрования](../sql-reference/statements/create/table.md#encryption-codecs), а затем добавьте к элементу, который нужно зашифровать, атрибут `encrypted_by` со значением в виде имени кодека шифрования.

В отличие от атрибутов `from_zk`, `from_env` и `incl`, а также элемента `include`, в предобработанном файле подстановка (то есть расшифровка зашифрованного значения) не выполняется.
Расшифровка происходит только на этапе выполнения в серверном процессе.

Например:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex>00112233445566778899aabbccddeeff</key_hex>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

Атрибуты [`from_env`](#from_env_zk) и [`from_zk`](#from_env_zk) также можно использовать для `encryption_codecs`:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_env="CLICKHOUSE_KEY_HEX"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

    <interserver_http_credentials>
        <user>admin</user>
        <password encrypted_by="AES_128_GCM_SIV">961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85</password>
    </interserver_http_credentials>

</clickhouse>
```

Ключи шифрования и зашифрованные значения можно задать в любом из этих файлов конфигурации.

Пример файла `config.xml` приведён ниже:

```xml
<clickhouse>

    <encryption_codecs>
        <aes_128_gcm_siv>
            <key_hex from_zk="/clickhouse/aes128_key_hex"/>
        </aes_128_gcm_siv>
    </encryption_codecs>

</clickhouse>
```

Пример `users.xml`:

```xml
<clickhouse>

    <users>
        <test_user>
            <password encrypted_by="AES_128_GCM_SIV">96280000000D000000000030D4632962295D46C6FA4ABF007CCEC9C1D0E19DA5AF719C1D9A46C446</password>
            <profile>default</profile>
        </test_user>
    </users>

</clickhouse>
```

Чтобы зашифровать значение, можно использовать программу `encrypt_decrypt` (пример):

```bash
./encrypt_decrypt /etc/clickhouse-server/config.xml -e AES_128_GCM_SIV abcd
```

```text
961F000000040000000000EEDDEF4F453CFE6457C4234BD7C09258BD651D85
```

Даже при использовании зашифрованных элементов конфигурации они всё равно отображаются в предобработанном файле конфигурации.
Если это создаёт проблему для вашего развертывания ClickHouse, есть два варианта: либо установить для предобработанного файла права доступа 600, либо использовать атрибут `hide_in_preprocessed`.

Например:

```xml
<clickhouse>

    <interserver_http_credentials hide_in_preprocessed="true">
        <user>admin</user>
        <password>secret</password>
    </interserver_http_credentials>

</clickhouse>
```

<div id="user-settings">
  ## Настройки пользователей
</div>

В файле `config.xml` можно указать отдельную конфигурацию с пользовательскими настройками, профилями и квотами. Относительный путь к этой конфигурации задаётся в элементе `users_config`. По умолчанию используется `users.xml`. Если `users_config` не указан, пользовательские настройки, профили и квоты задаются непосредственно в `config.xml`.

Пользовательскую конфигурацию можно разделить на отдельные файлы, аналогично `config.xml` и `config.d/`.
Имя каталога определяется значением настройки `users_config` без постфикса `.xml` с добавлением `.d`.
По умолчанию используется каталог `users.d`, так как для `users_config` по умолчанию задано значение `users.xml`.

Обратите внимание, что файлы конфигурации сначала [объединяются](#merging) с учётом настроек, а уже после этого обрабатываются директивы include.

<div id="example">
  ## Пример XML
</div>

Например, для каждого пользователя можно создать отдельный файл конфигурации:

```bash
$ cat /etc/clickhouse-server/users.d/alice.xml
```

```xml
<clickhouse>
    <users>
      <alice>
          <profile>analytics</profile>
            <networks>
                  <ip>::/0</ip>
            </networks>
          <password_sha256_hex>...</password_sha256_hex>
          <quota>analytics</quota>
      </alice>
    </users>
</clickhouse>
```

<div id="example-1">
  ## Примеры YAML
</div>

Здесь показана конфигурация по умолчанию в формате YAML: [`config.yaml.example`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/server/config.yaml.example).

Между форматами YAML и XML есть некоторые различия в конфигурациях ClickHouse.
Ниже приведены рекомендации по написанию конфигурации в формате YAML.

XML-тег с текстовым значением представляется в YAML как пара ключ-значение

```yaml
key: value
```

Соответствующий XML:

```xml
<key>value</key>
```

Вложенный XML-узел представляется YAML-отображением:

```yaml
map_key:
  key1: val1
  key2: val2
  key3: val3
```

Соответствующий XML:

```xml
<map_key>
    <key1>val1</key1>
    <key2>val2</key2>
    <key3>val3</key3>
</map_key>
```

Чтобы задать один и тот же XML-тег несколько раз, используйте YAML-последовательность:

```yaml
seq_key:
  - val1
  - val2
  - key1: val3
  - map:
      key2: val4
      key3: val5
```

Соответствующий XML:

```xml
<seq_key>val1</seq_key>
<seq_key>val2</seq_key>
<seq_key>
    <key1>val3</key1>
</seq_key>
<seq_key>
    <map>
        <key2>val4</key2>
        <key3>val5</key3>
    </map>
</seq_key>
```

Чтобы указать XML-атрибут, можно использовать ключ атрибута с префиксом `@`. Обратите внимание, что символ `@` зарезервирован стандартом YAML, поэтому его нужно заключать в двойные кавычки:

```yaml
map:
  "@attr1": value1
  "@attr2": value2
  key: 123
```

Соответствующий XML:

```xml
<map attr1="value1" attr2="value2">
    <key>123</key>
</map>
```

Также можно использовать атрибуты в YAML-последовательности:

```yaml
seq:
  - "@attr1": value1
  - "@attr2": value2
  - 123
  - abc
```

Соответствующий XML:

```xml
<seq attr1="value1" attr2="value2">123</seq>
<seq attr1="value1" attr2="value2">abc</seq>
```

Описанный выше синтаксис не позволяет представить в YAML текстовые узлы XML с атрибутами XML. Этот особый случай можно задать с помощью
ключа атрибута `#text`:

```yaml
map_key:
  "@attr1": value1
  "#text": value2
```

Соответствующий XML:

```xml
<map_key attr1="value1">value2</map>
```

<div id="implementation-details">
  ## Подробности реализации
</div>

Для каждого файла конфигурации сервер при запуске также генерирует файлы `file-preprocessed.xml`. Эти файлы содержат все выполненные подстановки и переопределения и предназначены только для ознакомления. Если в файлах конфигурации использовались подстановки ZooKeeper, но на момент запуска сервера ZooKeeper недоступен, сервер загружает конфигурацию из предобработанного файла.

Сервер отслеживает изменения в файлах конфигурации, а также в файлах и узлах ZooKeeper, которые использовались при выполнении подстановок и переопределений, и динамически перезагружает настройки пользователей и кластеров. Это означает, что вы можете изменять кластер, пользователей и их настройки без перезапуска сервера.