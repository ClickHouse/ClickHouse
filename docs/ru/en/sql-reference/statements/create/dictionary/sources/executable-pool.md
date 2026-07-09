---
slug: /sql-reference/statements/create/dictionary/sources/executable-pool
title: 'Источник словаря «Executable Pool»'
sidebar_position: 4
sidebar_label: 'Executable Pool'
description: 'Настройте «Executable Pool» как источник словаря в ClickHouse.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Executable pool позволяет загружать данные из пула процессов.
Этот источник не работает со структурами словарей, которым требуется загружать все данные из источника.

Executable pool работает, если словарь [хранится](../layouts/#storing-dictionaries-in-memory) с использованием одной из следующих структур:

* `cache`
* `complex_key_cache`
* `ssd_cache`
* `complex_key_ssd_cache`
* `direct`
* `complex_key_direct`

Executable pool запускает пул процессов с указанной командой и поддерживает их работу, пока они не завершатся. Программа должна читать данные из STDIN, пока они доступны, и выводить результат в STDOUT. Она может ожидать следующий блок данных из STDIN. ClickHouse не закрывает STDIN после обработки блока данных, а при необходимости передаёт следующий фрагмент данных. Исполняемый скрипт должен быть готов к такому способу обработки данных — он должен опрашивать STDIN и заранее сбрасывать данные в STDOUT.

Пример настроек:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE_POOL(
        command 'while read key; do printf "$key\tData for key $key\n"; done'
        format 'TabSeparated'
        pool_size 10
        max_command_execution_time 10
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="Файл конфигурации">
    ```xml
    <source>
        <executable_pool>
            <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
            <format>TabSeparated</format>
            <pool_size>10</pool_size>
            <max_command_execution_time>10<max_command_execution_time>
            <implicit_key>false</implicit_key>
        </executable_pool>
    </source>
    ```
  </TabItem>
</Tabs>

Параметры настройки:

| Setting                       | Description                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | Абсолютный путь к исполняемому файлу или имя файла (если каталог программы указан в `PATH`).                                                                                                                                                                                                                                                                                                                                            |
| `format`                      | Формат файла. Поддерживаются все форматы, описанные в разделе [Formats](/ru/sql-reference/formats).                                                                                                                                                                                                                                                                                                                                        |
| `pool_size`                   | Размер пула. Если для `pool_size` указано значение 0, размер пула не ограничивается. Значение по умолчанию — `16`.                                                                                                                                                                                                                                                                                                                      |
| `command_termination_timeout` | Исполняемый скрипт должен содержать основной цикл чтения и записи. После удаления словаря канал закрывается, и у исполняемого файла будет `command_termination_timeout` секунд на завершение работы, прежде чем ClickHouse отправит дочернему процессу сигнал SIGTERM. Указывается в секундах. Значение по умолчанию — `10`. Необязательный параметр.                                                                                   |
| `max_command_execution_time`  | Максимальное время выполнения команды исполняемого скрипта для обработки блока данных. Указывается в секундах. Значение по умолчанию — `10`. Необязательный параметр.                                                                                                                                                                                                                                                                   |
| `command_read_timeout`        | Тайм-аут чтения данных из stdout команды в миллисекундах. Значение по умолчанию — `10000`. Необязательный параметр.                                                                                                                                                                                                                                                                                                                     |
| `command_write_timeout`       | Тайм-аут записи данных в stdin команды в миллисекундах. Значение по умолчанию — `10000`. Необязательный параметр.                                                                                                                                                                                                                                                                                                                       |
| `implicit_key`                | Файл исполняемого источника может возвращать только значения, а соответствие запрошенным ключам определяется неявно по порядку строк в результате. Значение по умолчанию — `false`. Необязательный параметр.                                                                                                                                                                                                                            |
| `execute_direct`              | Если `execute_direct` = `1`, то `command` будет искаться в папке user&#95;scripts, указанной в [user&#95;scripts&#95;path](/ru/operations/server-configuration-parameters/settings#user_scripts_path). Дополнительные аргументы скрипта можно указать через пробел. Пример: `script_name arg1 arg2`. Если `execute_direct` = `0`, `command` передаётся как аргумент для `bin/sh -c`. Значение по умолчанию — `1`. Необязательный параметр. |
| `send_chunk_header`           | Определяет, нужно ли отправлять количество строк перед передачей фрагмента данных процессу. Значение по умолчанию — `false`. Необязательный параметр.                                                                                                                                                                                                                                                                                   |

Этот источник словаря можно настроить только через XML-конфигурацию. Создание словарей с исполняемым источником через DDL отключено, поскольку в противном случае пользователь БД мог бы выполнять произвольный бинарный файл на узле ClickHouse.