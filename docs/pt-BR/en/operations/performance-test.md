---
description: 'Guia para testar e fazer benchmarking do desempenho do hardware com ClickHouse'
sidebar_label: 'Teste de hardware'
sidebar_position: 54
slug: /operations/performance-test
title: 'Como testar seu hardware com ClickHouse'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Você pode executar um teste básico de desempenho do ClickHouse em qualquer servidor sem precisar instalar pacotes do ClickHouse.

<div id="automated-run">
  ## Execução automatizada
</div>

Você pode executar o teste de desempenho com um único script.

1. Baixe o script.

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. Execute o script.

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. Copie a saída e envie para feedback@clickhouse.com

Todos os resultados são publicados aqui: https://clickhouse.com/benchmark/hardware/