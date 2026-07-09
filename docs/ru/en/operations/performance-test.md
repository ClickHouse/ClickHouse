---
description: 'Руководство по тестированию оборудования и бенчмаркингу его производительности с помощью ClickHouse'
sidebar_label: 'Тестирование оборудования'
sidebar_position: 54
slug: /operations/performance-test
title: 'Как протестировать оборудование с помощью ClickHouse'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Вы можете выполнить базовый тест производительности ClickHouse на любом сервере без установки пакетов ClickHouse.

<div id="automated-run">
  ## Автоматический запуск
</div>

Вы можете запустить бенчмарк одним скриптом.

1. Скачайте скрипт.

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. Запустите скрипт.

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. Скопируйте вывод и отправьте его на feedback@clickhouse.com

Все результаты опубликованы здесь: https://clickhouse.com/benchmark/hardware/