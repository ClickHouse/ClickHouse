---
description: 'Guía para probar y hacer benchmark del rendimiento del hardware con ClickHouse'
sidebar_label: 'Pruebas de hardware'
sidebar_position: 54
slug: /operations/performance-test
title: 'Cómo probar el hardware con ClickHouse'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Puede ejecutar una prueba básica de rendimiento de ClickHouse en cualquier servidor sin necesidad de instalar paquetes de ClickHouse.

<div id="automated-run">
  ## Ejecución automatizada
</div>

Puede ejecutar el benchmark con un único script.

1. Descargue el script.

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. Ejecute el script.

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. Copia la salida y envíala a feedback@clickhouse.com

Todos los resultados se publican aquí: https://clickhouse.com/benchmark/hardware/