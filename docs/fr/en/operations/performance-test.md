---
description: 'Guide pour tester et mesurer les performances du matériel avec ClickHouse'
sidebar_label: 'Tester le matériel'
sidebar_position: 54
slug: /operations/performance-test
title: 'Comment tester votre matériel avec ClickHouse'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Vous pouvez exécuter un test de performances basique de ClickHouse sur n’importe quel serveur, sans installer de paquets ClickHouse.

<div id="automated-run">
  ## Exécution automatisée
</div>

Vous pouvez exécuter le benchmark à l’aide d’un seul script.

1. Téléchargez le script.

```bash
wget https://raw.githubusercontent.com/ClickHouse/ClickBench/main/hardware/hardware.sh
```

2. Exécutez le script.

```bash
chmod a+x ./hardware.sh
./hardware.sh
```

3. Copiez le résultat et envoyez-le à feedback@clickhouse.com

Tous les résultats sont publiés ici : https://clickhouse.com/benchmark/hardware/