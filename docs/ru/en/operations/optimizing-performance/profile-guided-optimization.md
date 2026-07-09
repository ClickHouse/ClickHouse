---
description: 'Документация по оптимизации на основе профилирования'
sidebar_label: 'Оптимизация на основе профилирования (PGO)'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: 'Оптимизация на основе профилирования'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # Оптимизация на основе профилирования
</div>

Оптимизация на основе профилирования (PGO), — это метод оптимизации компилятора, при котором программа оптимизируется на основе профиля выполнения.

По результатам тестов PGO помогает повысить производительность ClickHouse. В частности, в наборе тестов ClickBench прирост QPS может достигать 15%. Более подробные результаты доступны [здесь](https://pastebin.com/xbue3HMU). Прирост производительности зависит от вашей типичной рабочей нагрузки — результаты могут быть как лучше, так и хуже.

Подробнее о PGO в ClickHouse можно прочитать в соответствующем GitHub [issue](https://github.com/ClickHouse/ClickHouse/issues/44567).

<div id="how-to-build-clickhouse-with-pgo">
  ## Как собрать ClickHouse с PGO?
</div>

Существует два основных вида PGO: [Instrumentation](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) и [Sampling](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (также известный как AutoFDO). В этом руководстве описывается Instrumentation PGO для ClickHouse.

1. Соберите ClickHouse в режиме Instrumentation. В Clang это можно сделать, передав параметр `-fprofile-generate` в `CXXFLAGS`.
2. Запустите instrumented-сборку ClickHouse на типичной рабочей нагрузке. Здесь следует использовать вашу обычную рабочую нагрузку. В качестве одного из вариантов можно использовать [ClickBench](https://github.com/ClickHouse/ClickBench) как пример такой рабочей нагрузки. ClickHouse в режиме Instrumentation может работать медленно, поэтому учитывайте это и не запускайте instrumented-сборку ClickHouse в окружениях, критичных к производительности.
3. Пересоберите ClickHouse ещё раз с флагом компилятора `-fprofile-use` и профилями, собранными на предыдущем шаге.

Более подробное руководство по применению PGO приведено в [документации](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization) Clang.

Если вы собираетесь собирать типичную рабочую нагрузку напрямую из окружения продакшн, рекомендуем попробовать Sampling PGO.