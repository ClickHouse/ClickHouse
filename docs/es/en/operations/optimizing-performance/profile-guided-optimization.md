---
description: 'Documentación sobre la optimización guiada por perfiles'
sidebar_label: 'Optimización guiada por perfiles (PGO)'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: 'Optimización guiada por perfiles'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # Optimización guiada por perfiles
</div>

La optimización guiada por perfiles (PGO) es una técnica de optimización del compilador en la que un programa se optimiza en función de su perfil de ejecución.

Según las pruebas, PGO ayuda a mejorar el rendimiento de ClickHouse. En concreto, se observan mejoras de hasta un 15 % en QPS en la suite de pruebas ClickBench. Los resultados más detallados están disponibles [aquí](https://pastebin.com/xbue3HMU). Las mejoras de rendimiento dependen de su workload habitual; puede obtener resultados mejores o peores.

Puede leer más información sobre PGO en ClickHouse en el [issue](https://github.com/ClickHouse/ClickHouse/issues/44567) correspondiente de GitHub.

<div id="how-to-build-clickhouse-with-pgo">
  ## ¿Cómo compilar ClickHouse con PGO?
</div>

Hay dos tipos principales de PGO: [Instrumentación](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) y [Muestreo](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (también conocido como AutoFDO). En esta guía se describe el PGO por instrumentación con ClickHouse.

1. Compila ClickHouse en modo instrumentado. En Clang, esto puede hacerse pasando la opción `-fprofile-generate` a `CXXFLAGS`.
2. Ejecuta ClickHouse instrumentado con una carga de trabajo de ejemplo. Aquí debes usar tu carga de trabajo habitual. Una opción es usar [ClickBench](https://github.com/ClickHouse/ClickBench) como carga de trabajo de ejemplo. ClickHouse en modo instrumentado puede funcionar lentamente, así que tenlo en cuenta y no ejecutes ClickHouse instrumentado en entornos donde el rendimiento sea crítico.
3. Vuelve a compilar ClickHouse con la opción del compilador `-fprofile-use` y los perfiles recopilados en el paso anterior.

En la [documentación](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization) de Clang encontrarás una guía más detallada sobre cómo aplicar PGO.

Si vas a recopilar una carga de trabajo de ejemplo directamente desde un entorno de producción, te recomendamos usar PGO por muestreo.