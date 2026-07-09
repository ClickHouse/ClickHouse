---
description: 'Documentation sur l’optimisation guidée par le profilage'
sidebar_label: 'Optimisation guidée par le profilage (PGO)'
sidebar_position: 54
slug: /operations/optimizing-performance/profile-guided-optimization
title: 'Optimisation guidée par le profilage'
doc_type: 'guide'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="profile-guided-optimization">
  # Optimisation guidée par le profilage
</div>

L’optimisation guidée par le profilage (PGO) est une technique d’optimisation du compilateur qui consiste à optimiser un programme en fonction de son profil d’exécution.

Selon les tests, la PGO permet d’améliorer les performances de ClickHouse. Nous observons des gains pouvant aller jusqu’à 15 % de QPS sur la suite de tests ClickBench. Des résultats plus détaillés sont disponibles [ici](https://pastebin.com/xbue3HMU). Les gains de performance dépendent de votre charge de travail habituelle : vous pouvez obtenir de meilleurs comme de moins bons résultats.

Pour en savoir plus sur la PGO dans ClickHouse, consultez l’[issue](https://github.com/ClickHouse/ClickHouse/issues/44567) GitHub correspondante.

<div id="how-to-build-clickhouse-with-pgo">
  ## Comment compiler ClickHouse avec PGO ?
</div>

Il existe deux grands types de PGO : [Instrumentation](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) et [Sampling](https://clang.llvm.org/docs/UsersManual.html#using-sampling-profilers) (également appelé AutoFDO). Ce guide décrit le PGO par Instrumentation avec ClickHouse.

1. Compilez ClickHouse en mode instrumenté. Avec Clang, cela peut se faire en passant l’option `-fprofile-generate` à `CXXFLAGS`.
2. Exécutez ClickHouse instrumenté sur une charge de travail représentative. Ici, vous devez utiliser votre charge de travail habituelle. L’une des approches possibles consiste à utiliser [ClickBench](https://github.com/ClickHouse/ClickBench) comme charge de travail représentative. ClickHouse en mode instrumentation peut être lent ; préparez-vous donc à cela et n’exécutez pas ClickHouse instrumenté dans des environnements où les performances sont critiques.
3. Recompilez ensuite ClickHouse avec le flag de compilateur `-fprofile-use` et les profils collectés à l’étape précédente.

Vous trouverez un guide plus détaillé sur l’utilisation de PGO dans la [documentation](https://clang.llvm.org/docs/UsersManual.html#profile-guided-optimization) de Clang.

Si vous prévoyez de collecter une charge de travail représentative directement depuis un environnement de production, nous vous recommandons d’essayer le PGO par Sampling.