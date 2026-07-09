---
description: 'Présentation du système d’intégration continue de ClickHouse'
sidebar_label: 'Intégration continue (intégration continue)'
sidebar_position: 55
slug: /development/continuous-integration
title: 'Intégration continue (intégration continue)'
doc_type: 'reference'
---

Lorsque vous soumettez une pull request, certaines vérifications automatisées sont exécutées sur votre code par le [système d’intégration continue (intégration continue)](tests.md#test-automation) de ClickHouse.
Cela se produit après qu’un mainteneur du dépôt (quelqu’un de l’équipe ClickHouse) a examiné votre code et ajouté le label `can be tested` à votre pull request.
Les résultats des vérifications sont affichés sur la page GitHub de la pull request, comme décrit dans la [documentation GitHub sur les vérifications](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/about-status-checks).
Si une vérification échoue, il se peut que vous deviez la corriger.
Cette page présente les vérifications que vous pouvez rencontrer et ce que vous pouvez faire pour les corriger.

Si l’échec de la vérification ne semble pas lié à vos modifications, il peut s’agir d’un échec transitoire ou d’un problème d’infrastructure.
Poussez un commit vide sur la pull request pour relancer les vérifications d’intégration continue :

```shell
git commit --allow-empty
git push
```

Si vous ne savez pas quoi faire, demandez de l’aide à un responsable du projet.

<div id="merge-with-master">
  ## Fusion avec master
</div>

Vérifie que la PR peut être fusionnée dans la branche `master`.
Sinon, la vérification échouera avec le message `Cannot fetch mergecommit`.
Pour corriger cette vérification, résolvez le conflit comme décrit dans la [documentation GitHub](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/resolving-a-merge-conflict-on-github), ou fusionnez la branche `master` dans la branche de votre pull request à l’aide de git.

<div id="docs-check">
  ## Vérification de la documentation
</div>

Essaie de générer le site de documentation de ClickHouse.
Cela peut échouer si vous avez modifié quelque chose dans la documentation.
La raison la plus probable est qu&#39;un lien croisé dans la documentation est incorrect.
Accédez au rapport de vérification et recherchez les messages `ERROR` et `WARNING`.

<div id="description-check">
  ## Vérification de la description
</div>

Vérifiez que la description de votre pull request respecte le modèle [PULL&#95;REQUEST&#95;TEMPLATE.md](https://github.com/ClickHouse/ClickHouse/blob/master/.github/PULL_REQUEST_TEMPLATE.md).
Vous devez préciser une catégorie de changelog pour votre modification (par exemple, Bug Fix) et rédiger un message destiné aux utilisateurs décrivant la modification pour [CHANGELOG.md](../whats-new/changelog/index.md)

<div id="docker-image">
  ## Image Docker
</div>

Crée les images Docker du serveur ClickHouse et de Keeper afin de vérifier qu’elles se génèrent correctement.

<div id="official-docker-library-tests">
  ### Tests de la bibliothèque officielle Docker
</div>

Exécute les tests de la [bibliothèque officielle Docker](https://github.com/docker-library/official-images/tree/master/test#alternate-config-files) pour vérifier que l’image Docker `clickhouse/clickhouse-server` fonctionne correctement.

Pour ajouter de nouveaux tests, créez un répertoire `ci/jobs/scripts/docker_server/tests/$test_name` et ajoutez-y le script `run.sh`.

Vous trouverez plus de détails sur ces tests dans la [documentation des scripts des jobs d’intégration continue](https://github.com/ClickHouse/ClickHouse/tree/master/ci/jobs/scripts/docker_server).

<div id="marker-check">
  ## Vérification Marker
</div>

Cette vérification indique que le système d’intégration continue a commencé à traiter la pull request.
Lorsqu’elle a le statut &#39;pending&#39;, cela signifie que toutes les vérifications n’ont pas encore été lancées.
Une fois toutes les vérifications lancées, son statut passe à &#39;success&#39;.

<div id="style-check">
  ## Vérification du style
</div>

Effectue diverses vérifications de style sur le code source. Chacun des sous-contrôles ci-dessous correspond à un `testname` dans [`ci/jobs/check_style.py`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/check_style.py) et peut être exécuté individuellement avec `--test <name>` (voir ci-dessous).

<div id="cpp">
  ##### cpp
</div>

Vérifications du style C++ basées sur des expressions régulières via [`check_cpp.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/check_cpp.sh). En cas d’échec, corrigez les problèmes conformément au [guide de style du code](style.md).

<div id="whitespace-check">
  ##### whitespace_check
</div>

Détecte les doubles espaces après les virgules en C++ qui ne relèvent pas de l’alignement des colonnes.

<div id="catch-all">
  ##### catch_all
</div>

Interdit `catch (...)` en dehors des destructeurs, de `main` et des points d’entrée du fuzzer, où il est dangereux d’ignorer une exception inconnue.

<div id="yamllint">
  ##### yamllint
</div>

Vérifie les fichiers YAML de workflow dans `.github/` à l’aide de `.yamllint`.

<div id="xmllint">
  ##### xmllint
</div>

Valide les fichiers XML dans `tests/` et `programs/`.

<div id="functional-tests-check">
  ##### functional_tests_check
</div>

Vérifie les tests sans état : les requêtes avec un filtre sur `event_date` doivent utiliser `>= yesterday()` plutôt que `today()` (pour éviter les comportements aléatoires autour de minuit), et les noms des fichiers de test ne doivent pas contenir `fail`.

<div id="test-numbers-check">
  ##### test_numbers_check
</div>

Signale d’importants écarts dans la numérotation des tests sans état (`tests/queries/0_stateless/<NNNNN>_*`).

<div id="symlinks">
  ##### liens symboliques
</div>

Détecte les liens symboliques cassés dans le dépôt.

<div id="various">
  ##### divers
</div>

Contrôles divers du dépôt via [`various_checks.sh`](https://github.com/ClickHouse/ClickHouse/blob/master/ci/jobs/scripts/check_style/various_checks.sh) : les requêtes sur `system.query_log` / `system.parts` / etc. doivent filtrer selon `currentDatabase`, les chemins ZooKeeper de `Replicated*MergeTree` doivent inclure un préfixe propre à chaque test, les répertoires de tests d’intégration doivent contenir `__init__.py`, pas de BOM UTF, pas de droits d’exécution sur les fichiers source ou de données, pas de tags `:latest` sur les images docker-compose tierces, et plus encore.

<div id="running-style-check-locally">
  ### Exécuter localement le job Style Vérification
</div>

Le job *Style Vérification* peut être exécuté intégralement dans un conteneur Docker avec :

```sh
python -m ci.praktika run "Style check"
```

Pour exécuter une vérification spécifique (par exemple, la vérification *cpp*) :

```sh
python -m ci.praktika run "Style check" --test cpp
```

Ces commandes téléchargent l’image Docker `clickhouse/style-test` et exécutent le job dans un environnement conteneurisé.
Aucune dépendance autre que Python 3 et Docker n’est nécessaire.

<div id="running-stateless-tests">
  ## Exécuter des tests sans état
</div>

Une instance ClickHouse installée localement avec la configuration par défaut peut convenir à certains cas de test, mais ne permet pas d’exécuter correctement toutes les requêtes de test. En intégration continue, chaque tâche installe une configuration spécifique de ClickHouse (par ex. S3 storage, Parallel Replicas), ce qui peut être fastidieux à reproduire manuellement. Pour éviter cela, vous pouvez reproduire localement n’importe quelle tâche d’intégration continue en utilisant la même orchestration que celle de l’intégration continue, sans aucune configuration manuelle.

<div id="ci-prerequisites">
  #### Prérequis
</div>

* Python 3 (bibliothèque standard uniquement)
* Docker

Installez Docker sur Ubuntu si nécessaire, puis reconnectez-vous :

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "ipv6": true,
  "ip6tables": true
}
EOF
sudo systemctl restart docker
```

<div id="run-ci-job-locally">
  #### Exécuter un job d’intégration continue en local
</div>

Choisissez le nom d’un job dans un rapport d’intégration continue et exécutez-le en local :

```bash
python -m ci.praktika run "<JOB_NAME>"
```

* Citez toujours le nom du job exactement tel qu’il apparaît dans le rapport d’intégration continue (il peut contenir des espaces et des virgules), par ex. : `"Stateless tests (amd_debug, parallel)"`. Cela applique la même configuration ClickHouse et exécute les mêmes tests qu’en intégration continue.
* L’architecture et le type de build dans le nom du job (par ex. `amd_debug`) sont des libellés propres à l’intégration continue. En local, ils n’ont aucun effet : le job utilisera le binaire que vous fournissez, sur l’architecture sur laquelle vous l’exécutez. Le nom du job détermine uniquement la configuration ClickHouse et l’ensemble de tests (sauf remplacement par `--test`).
* En intégration continue, les tests fonctionnels sont répartis en lots afin d’optimiser l’utilisation des ressources. Par exemple, `"Stateless tests (amd_debug, parallel)"` et `"Stateless tests (amd_debug, sequential)"` couvrent ensemble l’intégralité du périmètre : les tests compatibles avec l’exécution en parallèle sont lancés en parallèle, et le reste s’exécute de façon séquentielle. Cette répartition réduit la durée totale de l’intégration continue en maximisant le parallélisme lorsque c’est possible. Pour reproduire localement l’ensemble complet des tests, exécutez les deux lots.
* Il existe également un job d’intégration continue `"Fast test"` qui exécute un périmètre limité de tests fonctionnels pour vérifier les fonctionnalités de base de ClickHouse ; il utilise un build sans tous les modules optionnels et constitue le moyen le plus rapide de détecter les régressions. Vous pouvez l’exécuter localement de la même manière. Placez votre binaire ClickHouse dans l’un des chemins de recherche par défaut (`./ci/tmp/clickhouse`, `./build/programs/clickhouse` ou `./clickhouse`) — sinon, le job tentera d’abord de compiler ClickHouse :
  ```bash
  python -m ci.praktika run "Fast test"
  ```

<div id="run-specific-tests-within-ci-job">
  #### Exécuter des tests spécifiques dans un job d’intégration continue
</div>

Avec `--test`, le job prépare une configuration ClickHouse identique à celle utilisée en intégration continue, mais n’exécute que les tests sélectionnés.

```bash
python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
  --test 00001_select1
```

* Vous pouvez indiquer plusieurs noms de test :
  ```bash
  python -m ci.praktika run "Stateless tests (amd_debug, parallel)" \
    --test 00001_select1 00002_log_and_exception_messages_formatting
  ```
* Astuce : si n’importe quelle configuration de ClickHouse vous convient et que vous devez seulement exécuter des tests spécifiques, utilisez l’alias `functional` au lieu du nom complet du job :
  ```bash
  python -m ci.praktika run functional --test 00001_select1
  ```

<div id="additional-customization-options">
  #### Options de personnalisation supplémentaires
</div>

* `--path PATH` — chemin personnalisé vers le binaire ClickHouse. Par défaut, le runner recherche successivement dans : `./ci/tmp/clickhouse`, `./build/programs/clickhouse`, `./clickhouse`.
* `--count N` — répéter chaque test N fois.
* `--workers N` — remplace le calcul automatique du nombre de workers parallèles en fonction de la capacité de la machine.

<div id="build-check">
  ## Vérification de build
</div>

Compile ClickHouse dans différentes configurations afin de l’utiliser dans les étapes suivantes.

<div id="running-builds-locally">
  ### Exécuter les builds localement
</div>

Le build peut être exécuté localement dans un environnement de type intégration continue à l’aide de :

```bash
python -m ci.praktika run "<BUILD_JOB_NAME>"
```

Aucune dépendance autre que Python 3 et Docker n’est requise.

<div id="available-build-jobs">
  #### Jobs de build disponibles
</div>

Les noms des jobs de build sont exactement ceux qui apparaissent dans le rapport d’intégration continue :

**Builds AMD64 :**

* `Build (amd_debug)` - Build de débogage avec symboles
* `Build (amd_release)` - Build release optimisé
* `Build (amd_asan)` - Build avec Address Sanitizer
* `Build (amd_tsan)` - Build avec Thread Sanitizer
* `Build (amd_msan)` - Build avec Memory Sanitizer
* `Build (amd_ubsan)` - Build avec Undefined Behavior Sanitizer
* `Build (amd_binary)` - Build release rapide sans Thin LTO
* `Build (amd_compat)` - Build de compatibilité pour les anciens systèmes
* `Build (amd_musl)` - Build avec musl libc
* `Build (amd_darwin)` - Build macOS
* `Build (amd_freebsd)` - Build FreeBSD

**Builds ARM64 :**

* `Build (arm_release)` - Build release ARM64 optimisé
* `Build (arm_asan)` - Build ARM64 avec Address Sanitizer
* `Build (arm_coverage)` - Build ARM64 avec instrumentation de couverture
* `Build (arm_binary)` - Build release rapide ARM64 sans Thin LTO
* `Build (arm_darwin)` - Build macOS ARM64
* `Build (arm_v80compat)` - Build de compatibilité ARMv8.0

**Autres architectures :**

* `Build (ppc64le)` - PowerPC 64 bits Little Endian
* `Build (riscv64)` - RISC-V 64 bits
* `Build (s390x)` - IBM System/390 64 bits
* `Build (loongarch64)` - LoongArch 64 bits

Si le job réussit, les résultats du build seront disponibles dans le répertoire `<repo_root>/ci/tmp/build`.

**Remarque :** Pour les builds qui ne relèvent pas de la catégorie « Autres architectures » (qui utilisent la compilation croisée), l’architecture de votre machine locale doit correspondre au type de build afin de produire le build demandé par `BUILD_JOB_NAME`.

<div id="example-run-local">
  #### Exemple
</div>

Pour exécuter un build local de débogage :

```bash
python -m ci.praktika run "Build (amd_debug)"
```

Si l’approche ci-dessus ne fonctionne pas pour vous, utilisez les options CMake du journal de build et suivez le [processus général de build](../development/build.md).

<div id="functional-stateless-tests">
  ## Tests fonctionnels sans état
</div>

Exécute les [tests fonctionnels sans état](tests.md#functional-tests) sur les binaires ClickHouse compilés dans différentes configurations -- release, debug, avec des sanitizers, etc.
Consultez le rapport pour voir quels tests échouent, puis reproduisez l&#39;échec en local comme décrit [ici](/fr/development/tests#functional-tests).
Notez que vous devez utiliser la configuration de build appropriée pour reproduire le problème -- un test peut échouer avec AddressSanitizer, mais réussir en Debug.
Téléchargez le binaire depuis la [page des vérifications de build de l’intégration continue](/fr/install/advanced), ou compilez-le localement.

<div id="integration-tests">
  ## Tests d’intégration
</div>

Exécute les [tests d’intégration](tests.md#integration-tests).

<div id="bugfix-validate-check">
  ## Vérification de validation du correctif de bogue
</div>

Vérifie qu&#39;il existe soit un nouveau test (fonctionnel ou d’intégration), soit des tests modifiés qui échouent avec le binaire compilé sur la branche master.
Cette vérification se déclenche lorsqu’une pull request porte le label &quot;pr-bugfix&quot;.

<div id="stress-test">
  ## Test de stress
</div>

Exécute des tests fonctionnels sans état en parallèle depuis plusieurs clients afin de détecter les erreurs liées à la concurrence. En cas d&#39;échec :

* Corrigez d&#39;abord tous les autres échecs de test ;
  * Consultez le rapport pour trouver les logs du serveur et les vérifier afin d&#39;identifier les causes possibles
    de l&#39;erreur.

<div id="compatibility-check">
  ## Vérification de compatibilité
</div>

Vérifie que le binaire `clickhouse` s’exécute sur des distributions utilisant d’anciennes versions de libc.
En cas d’échec, demandez l’aide d’un mainteneur.

<div id="ast-fuzzer">
  ## AST fuzzer
</div>

Exécute des requêtes générées de façon aléatoire afin de détecter des erreurs dans le programme.
En cas d’échec, demandez de l’aide à un mainteneur.

<div id="performance-tests">
  ## Tests de performance
</div>

Mesurez l’évolution des performances des requêtes.
C’est la vérification la plus longue et son exécution prend un peu moins de 6 heures.
Le rapport de test de performance est décrit en détail [ici](https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/scripts/README.md#how-to-read-the-report).