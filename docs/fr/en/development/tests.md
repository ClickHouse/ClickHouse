---
description: 'Guide pour tester ClickHouse et exécuter la suite de tests'
sidebar_label: 'Tests'
sidebar_position: 40
slug: /development/tests
title: 'Tester ClickHouse'
doc_type: 'guide'
---

<div id="test-types">
  ## Types de tests
</div>

ClickHouse comprend les types de tests suivants :

* [Tests fonctionnels](#functional-tests) - un ensemble de requêtes et de scripts comprenant les sous-ensembles suivants, qui se recoupent
  * [Fast test](#running-fast-tests) - le sous-ensemble minimal
  * [Tests sans état](#running-stateless-tests), qui ne nécessitent pas de remplir les bases de données avec des données
  * Tests séquentiels, qui ne peuvent pas être exécutés en parallèle
* [Tests d&#39;intégration](#integration-tests), exécutés par `pytest` dans un cluster
* [Tests unitaires](#unit-tests)
* [Tests de performance](#performance-tests)
* [Tests de compilation](#build-tests)
* [Sanitizers](#sanitizers)
* [Fuzzers](#fuzzing)
  et quelques autres ; voir les sections ci-dessous.

<div id="functional-tests">
  ## Tests fonctionnels
</div>

Les tests fonctionnels sont les plus simples et les plus pratiques à utiliser.
La plupart des fonctionnalités de ClickHouse peuvent être testées à l’aide de tests fonctionnels, et leur utilisation est obligatoire pour toute modification du code de ClickHouse qui peut être testée de cette manière.

Chaque test fonctionnel envoie une ou plusieurs requêtes au serveur ClickHouse en cours d’exécution et compare le résultat à la référence.

Les tests se trouvent dans le répertoire `./tests/queries`.

Chaque test peut être de l’un des deux types suivants : `.sql` et `.sh`.

* Un test `.sql` est un simple script SQL envoyé par pipe à `clickhouse-client`.
* Un test `.sh` est un script exécuté directement.

Les tests SQL sont généralement préférables aux tests `.sh`.
Vous ne devez utiliser des tests `.sh` que lorsque vous devez tester une fonctionnalité qui ne peut pas l’être en SQL pur, par exemple pour envoyer des données d’entrée par pipe à `clickhouse-client` ou pour tester `clickhouse-local`.

:::note
Une erreur fréquente lors du test des types de données `DateTime` et `DateTime64` consiste à supposer que le serveur ClickHouse utilise un fuseau horaire spécifique (par ex. &quot;UTC&quot;). Ce n’est pas le cas : les fuseaux horaires utilisés lors des exécutions de tests en intégration continue
sont délibérément randomisés. La solution de contournement la plus simple consiste à spécifier explicitement le fuseau horaire des valeurs de test, par ex. `toDateTime64(val, 3, 'Europe/Amsterdam')`.
:::

<div id="running-a-test-locally">
  ### Exécuter un test en local
</div>

Démarrez le serveur ClickHouse en local, à l’écoute sur le port par défaut (9000).
Pour exécuter, par exemple, le test `01428_hash_set_nan_key`, placez-vous dans le dossier du dépôt et lancez la commande suivante :

```sh
PATH=<path to clickhouse-client>:$PATH tests/clickhouse-test 01428_hash_set_nan_key
```

Les résultats des tests (`stderr` et `stdout`) sont écrits dans les fichiers `01428_hash_set_nan_key.[stderr|stdout]`, qui se trouvent dans le même répertoire que le test lui-même (pour `queries/0_stateless/foo.sql`, la sortie se trouvera dans `queries/0_stateless/foo.stdout`).

Consultez `tests/clickhouse-test --help` pour afficher toutes les options de `clickhouse-test`.
Vous pouvez exécuter tous les tests ou seulement un sous-ensemble en fournissant un filtre sur les noms de test : `./clickhouse-test substring`.
Il existe également des options pour exécuter les tests en parallèle ou dans un ordre aléatoire.

<div id="running-tests-on-macos">
  #### Exécuter les tests sur macOS (Darwin)
</div>

De nombreux tests fonctionnels s’appuient sur des utilitaires GNU en ligne de commande (`timeout`, `head`, `sed`, `grep`, `date`, etc.). macOS fournit les variantes BSD de ces outils, dont le comportement et les options diffèrent (par exemple, BSD `head` rejette `head -c 1G`, BSD `ps` ne prend pas en charge les options longues `--`, et `timeout` n’existe tout simplement pas). Exécuter les tests avec les outils BSD provoque des échecs parasites.

Les exécuteurs d’intégration continue macOS installent les outils GNU via Homebrew et les placent avant les outils BSD dans le `PATH`. Reproduisez la même configuration en local :

```sh
brew install coreutils gnu-sed grep
export PATH="$(brew --prefix)/opt/coreutils/libexec/gnubin:$(brew --prefix)/opt/gnu-sed/libexec/gnubin:$(brew --prefix)/opt/grep/libexec/gnubin:$PATH"
```

`coreutils` fournit les versions GNU de `timeout`, `head`, `date` et autres utilitaires du même type ; `gnu-sed` et `grep` fournissent les versions GNU de `sed` et `grep`. Après cela, `which timeout head sed grep` devrait renvoyer les chemins `gnubin`.

<div id="running-fast-tests">
  ### Exécution des tests rapides
</div>

Vous aurez peut-être besoin d’une machine assez puissante pour exécuter un sous-ensemble de tests (appelé &quot;Fast test&quot;). La procédure ci-dessous fonctionne sur une instance AWS Ubuntu amd64 `t3.2xlarge` disposant de 100 Go de stockage.

1. Installez les prérequis, puis reconnectez-vous.

```sh
sudo apt-get update
sudo apt-get install docker.io
sudo usermod -aG docker "$USER"
```

2. Téléchargez le code source.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. Compilez le code et exécutez les &quot;fast tests&quot;.

```sh
python -m ci.praktika run fast
```

Vous devriez voir

```sh
Failed: 0, Passed: 7394, Skipped: 1795
```

Si vous laissez l’exécution sans surveillance, vous pouvez utiliser `nohup` ou `disown` pour qu’elle continue à s’exécuter même si la connexion `ssh` est interrompue.

<div id="running-stateless-tests">
  ### Exécuter des tests sans état
</div>

Vous aurez peut-être besoin d’une machine assez puissante pour exécuter des tests sans état. La procédure ci-dessous fonctionne sur une instance AWS Ubuntu amd64 `m7i.8xlarge` disposant de 200 Go de stockage.

1. Installez les prérequis, puis reconnectez-vous.

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

2. Récupérez le code source.

```sh
git clone --single-branch https://github.com/ClickHouse/ClickHouse
cd ClickHouse
```

3. Compilez le code.

```sh
python -m ci.praktika run build_debug
cp ci/tmp/build/programs/clickhouse ci/tmp
```

4. Exécutez les tests sans état, qui peuvent être exécutés en parallèle.

```sh
python -m ci.praktika run functional
```

Vous devriez obtenir

```sh
Failed: 0, Passed: 8497, Skipped: 103
```

Remarque : les commandes `python -m ci.praktika run` lancent une tâche spécifique d’intégration continue ; vous pouvez en savoir plus sur l’intégration continue de ClickHouse [ici](continuous-integration.md#running-stateless-tests).

<div id="adding-a-new-test">
  ### Ajout d’un nouveau test
</div>

Pour ajouter un nouveau test, créez d’abord un fichier `.sql` ou `.sh` dans le répertoire `queries/0_stateless`.
Générez ensuite le fichier `.reference` correspondant avec `clickhouse-client < 12345_test.sql > 12345_test.reference` ou `./12345_test.sh > ./12345_test.reference`.

Les tests doivent uniquement créer, supprimer, interroger, etc. des tables dans la base de données `test`, qui est automatiquement créée au préalable.
Il est possible d’utiliser des tables temporaires.

Pour reproduire localement le même environnement qu’en intégration continue, installez les configurations de test (elles utiliseront une implémentation simulée de ZooKeeper et ajusteront certains paramètres)

```sh
cd <repository>/tests/config
sudo ./install.sh
```

:::note
Les tests doivent être

* minimaux : ne créer que les tables, colonnes et le niveau de complexité strictement nécessaires,
* rapides : ne pas prendre plus de quelques secondes (mieux encore : moins d’une seconde),
* corrects et déterministes : échouer si et seulement si la fonctionnalité testée ne fonctionne pas,
* isolés/sans état : ne pas dépendre de l’environnement ni du timing,
* exhaustifs : couvrir les cas limites comme les zéros, les valeurs NULL, les ensembles vides et les exceptions (tests négatifs ; utilisez pour cela la syntaxe `-- { serverError xyz }` et `-- { clientError xyz }`),
* nettoyer les tables à la fin du test (en cas de restes),
* s’assurer que les autres tests ne vérifient pas la même chose (autrement dit, faites d’abord un `grep`).
  :::

<div id="templated-tests-with-jinja">
  ### Tests templatisés avec Jinja
</div>

Un test `.sql` peut être écrit sous forme de template [Jinja2](https://jinja.palletsprojects.com/) en ajoutant le suffixe `.j2` au nom du fichier : `foo.sql` devient ainsi `foo.sql.j2`. Avant d&#39;exécuter le test, `clickhouse-test` convertit le template en un script `.sql` classique, puis exécute le résultat.

C&#39;est utile lorsqu&#39;un test répète la même requête avec de légères variations : une boucle génère les requêtes à partir d&#39;un template compact, au lieu de toutes les écrire à la main. Les constructions les plus courantes sont :

* `{% for ... %} ... {% endfor %}` pour répéter un bloc,
* `{{ expression }}` pour insérer une valeur dans la sortie,
* `-%}` et `{%-` pour supprimer les espaces adjacents afin que le script généré reste propre.

Par exemple, ce template :

```sql
{% for type in ['UInt8', 'UInt16', 'UInt32'] -%}
SELECT toTypeName(0::{{ type }});
{% endfor -%}
```

donne :

```sql
SELECT toTypeName(0::UInt8);
SELECT toTypeName(0::UInt16);
SELECT toTypeName(0::UInt32);
```

La sortie attendue peut être fournie soit sous la forme d’un simple fichier `<name>.reference` contenant les résultats entièrement développés, soit sous la forme d’un template `<name>.reference.j2`, que `clickhouse-test` interprète de la même manière avant la comparaison. Utilisez la forme avec template lorsque la sortie attendue suit elle aussi un schéma répétitif. Pour plus d’exemples, consultez les fichiers `*.sql.j2` existants dans `tests/queries/0_stateless/`.

<div id="restricting-test-runs">
  ### Limiter l’exécution des tests
</div>

Un test peut avoir zéro, un ou plusieurs *tags* indiquant les restrictions sur les contextes dans lesquels il s’exécute en intégration continue.

Pour les tests `.sql`, les tags sont placés sur la première ligne sous forme de commentaire SQL :

```sql
-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: <provide_a_reason_for_the_tag_here>
-- no-replicated-database: <provide_a_reason_here>

SELECT 1
```

Pour les tests `.sh`, les tags sont indiqués sous forme de commentaire sur la deuxième ligne :

```bash
#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: <provide_a_reason_for_the_tag_here>
# - no-replicated-database: <provide_a_reason_here>
```

Liste des tags disponibles :

| Tag name                       | What it does                                                                               | Usage example                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| `disabled`                     | Le test n’est pas exécuté                                                                  |                                                                                                                       |
| `long`                         | Le temps d’exécution du test passe de 1 à 10 minutes                                       |                                                                                                                       |
| `deadlock`                     | Le test est exécuté en boucle pendant une longue période                                   |                                                                                                                       |
| `race`                         | Identique à `deadlock`. Préférez `deadlock`                                                |                                                                                                                       |
| `shard`                        | Le serveur doit écouter sur `127.0.0.*`                                                    |                                                                                                                       |
| `distributed`                  | Identique à `shard`. Préférez `shard`                                                      |                                                                                                                       |
| `global`                       | Identique à `shard`. Préférez `shard`                                                      |                                                                                                                       |
| `zookeeper`                    | Le test nécessite ZooKeeper ou ClickHouse Keeper pour s’exécuter                           | Le test utilise `ReplicatedMergeTree`                                                                                 |
| `replica`                      | Identique à `zookeeper`. Préférez `zookeeper`                                              |                                                                                                                       |
| `no-fasttest`                  | Le test n’est pas exécuté dans [Fast test](#test-types)                                    | Le test utilise le moteur de table `MySQL`, qui est désactivé dans Fast test                                          |
| `fasttest-only`                | Le test est exécuté uniquement dans [Fast test](#test-types)                               |                                                                                                                       |
| `no-[asan, tsan, msan, ubsan]` | Désactive les tests dans les builds avec [sanitizers](#sanitizers)                         | Le test est exécuté sous QEMU, qui ne fonctionne pas avec les sanitizers                                              |
| `no-replicated-database`       | Désactive le test lorsque la base de données par défaut utilise `ReplicatedDatabaseEngine` |                                                                                                                       |
| `no-ordinary-database`         | Désactive le test lorsque le moteur de la base de données par défaut est `Ordinary`        |                                                                                                                       |
| `no-parallel`                  | Désactive l’exécution d’autres tests en parallèle avec celui-ci                            | Le test lit des tables `system` et des invariants peuvent être violés                                                 |
| `no-parallel-replicas`         | Désactive le test lorsque les répliques parallèles sont activées                           |                                                                                                                       |
| `no-debug`                     | Désactive les tests dans les builds Debug                                                  |                                                                                                                       |
| `no-release`                   | Désactive les tests dans les builds Release                                                |                                                                                                                       |
| `no-darwin`                    | Désactive le test sur MacOS (Darwin)                                                       | Le test s’appuie sur des fonctionnalités propres à Linux, comme les requêtes distribuées, `procfs` ou le serveur HTTP |

Les options suivantes sont également prises en charge : `no-polymorphic-parts`, `no-random-settings`, `no-random-merge-tree-settings`, `no-backward-compatibility-check`, `no-cpu-x86_64`, `no-cpu-aarch64`, `no-cpu-ppc64le`, `no-s3-storage`.

En plus des paramètres ci-dessus, vous pouvez utiliser les flags `USE_*` de `system.build_options` pour indiquer l’utilisation de fonctionnalités ClickHouse particulières.
Par exemple, si votre test utilise une table MySQL, vous devez ajouter le tag `use-mysql`.

<div id="specifying-limits-for-random-settings">
  ### Définir des limites pour les paramètres aléatoires
</div>

Un test peut définir des valeurs minimales et maximales autorisées pour les paramètres pouvant être randomisés pendant son exécution.

Pour les tests `.sh`, les limites sont indiquées sous forme de commentaire sur la ligne à côté des tags, ou sur la deuxième ligne si aucun tag n&#39;est spécifié :

```bash
#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
```

Pour les tests `.sql`, les tags sont indiqués dans un commentaire SQL sur la ligne à côté de tags ou sur la première ligne :

```sql
-- Tags: no-fasttest
-- Random settings limits: max_block_size=(1000, 10000); index_granularity=(100, None)
SELECT 1
```

Si vous ne devez spécifier qu’une seule limite, vous pouvez utiliser `None` pour l’autre.

<div id="choosing-the-test-name">
  ### Choisir le nom du test
</div>

Le nom du test commence par un préfixe à cinq chiffres, suivi d&#39;un nom descriptif, tel que `00422_hash_function_constexpr.sql`.
Pour choisir le préfixe, trouvez le plus grand préfixe déjà présent dans le répertoire, puis augmentez-le de un.

```sh
ls tests/queries/0_stateless/[0-9]*.reference | tail -n 1
```

En attendant, d’autres tests pourront être ajoutés avec le même préfixe numérique, mais ce n’est pas gênant et cela ne posera aucun problème : vous n’aurez pas à le modifier plus tard.

<div id="checking-for-an-error-that-must-occur">
  ### Vérifier qu’une erreur se produit bien
</div>

Il est parfois utile de vérifier qu’une erreur du serveur est bien renvoyée pour une requête incorrecte. Nous prenons en charge des annotations spéciales à cet effet dans les tests SQL, sous la forme suivante :

```sql
SELECT x; -- { serverError 49 }
```

Ce test garantit que le serveur renvoie une erreur de code 49 indiquant que la colonne `x` est inconnue.
S&#39;il n&#39;y a pas d&#39;erreur, ou si l&#39;erreur est différente, le test échouera.
Si vous voulez vous assurer qu&#39;une erreur se produit côté client, utilisez plutôt l&#39;annotation `clientError`.

Ne vérifiez pas le libellé exact du message d&#39;erreur : il peut changer à l&#39;avenir, et le test échouerait inutilement.
Vérifiez uniquement le code d&#39;erreur.
Si le code d&#39;erreur existant n&#39;est pas suffisamment précis pour vos besoins, envisagez d&#39;en ajouter un nouveau.

<div id="testing-a-distributed-query">
  ### Tester une requête distribuée
</div>

Si vous souhaitez utiliser des requêtes distribuées dans des tests fonctionnels, vous pouvez utiliser la fonction de table `remote` avec les adresses `127.0.0.{1..2}` afin de permettre au serveur de s&#39;interroger lui-même ; vous pouvez également utiliser des clusters de test prédéfinis dans le fichier de configuration du serveur, comme `test_shard_localhost`.
N&#39;oubliez pas d&#39;ajouter les mots `shard` ou `distributed` au nom du test, afin qu&#39;il soit exécuté en intégration continue avec les bonnes configurations, dans lesquelles le serveur est configuré pour prendre en charge les requêtes distribuées.

<div id="working-with-temporary-files">
  ### Utilisation des fichiers temporaires
</div>

Dans certains tests shell, il peut être nécessaire de créer un fichier à la volée pour l’utiliser.
Gardez à l’esprit que certaines vérifications d’intégration continue exécutent les tests en parallèle. Si vous créez ou supprimez un fichier temporaire dans votre script sans lui donner un nom unique, cela peut faire échouer certaines vérifications d’intégration continue, comme Flaky.
Pour éviter ce problème, utilisez la variable d’environnement `$CLICKHOUSE_TEST_UNIQUE_NAME` afin de donner aux fichiers temporaires un nom unique pour le test en cours d’exécution.
Vous aurez ainsi la garantie que le fichier créé pendant la préparation ou supprimé pendant le nettoyage n’est utilisé que par ce test, et non par un autre test exécuté en parallèle.

<div id="known-bugs">
  ## Bogues connus
</div>

Lorsque nous connaissons des bogues facilement reproductibles par des tests fonctionnels, nous plaçons les tests fonctionnels correspondants dans le répertoire `tests/queries/bugs`.
Ces tests sont déplacés vers `tests/queries/0_stateless` une fois les bogues corrigés.

<div id="integration-tests">
  ## Tests d’intégration
</div>

Les tests d’intégration permettent de tester ClickHouse dans une configuration en cluster, ainsi que les interactions de ClickHouse avec d’autres serveurs comme MySQL, Postgres ou MongoDB.
Ils sont utiles pour émuler des partitions réseau, des pertes de paquets, etc.
Ces tests sont exécutés sous Docker et créent plusieurs conteneurs exécutant différents logiciels.

Consultez `tests/integration/README.md` pour savoir comment exécuter ces tests.

Notez que l’intégration de ClickHouse avec des pilotes tiers n’est pas testée.
Par ailleurs, nous ne disposons actuellement pas de tests d’intégration pour nos pilotes JDBC et ODBC.

<div id="unit-tests">
  ## Tests unitaires
</div>

Les tests unitaires sont utiles lorsque vous souhaitez tester non pas ClickHouse dans son ensemble, mais une bibliothèque ou une classe isolée.
Vous pouvez activer ou désactiver la compilation des tests avec l&#39;option CMake `ENABLE_TESTS`.
Les tests unitaires (ainsi que d&#39;autres programmes de test) se trouvent dans les sous-répertoires `tests` du code source.
Pour exécuter les tests unitaires, saisissez `ninja test`.
Certains tests utilisent `gtest`, mais d&#39;autres sont simplement des programmes qui renvoient un code de sortie non nul en cas d&#39;échec du test.

Il n&#39;est pas nécessaire d&#39;avoir des tests unitaires si le code est déjà couvert par des tests fonctionnels (et les tests fonctionnels sont généralement bien plus simples à utiliser).

Vous pouvez exécuter des tests `gtest` individuels en appelant directement l&#39;exécutable, par exemple :

```bash
$ ./src/unit_tests_dbms --gtest_filter=LocalAddress*
```

<div id="performance-tests">
  ## Tests de performance
</div>

Les tests de performance permettent de mesurer et de comparer les performances de certaines parties isolées de ClickHouse sur des requêtes synthétiques.
Les tests de performance se trouvent dans `tests/performance/`.
Chaque test est représenté par un fichier `.xml` contenant une description du cas de test.
Les tests sont exécutés avec l’outil `docker/test/performance-comparison`. Consultez le fichier readme pour la syntaxe d’utilisation.

Chaque test exécute une ou plusieurs requêtes (éventuellement avec des combinaisons de paramètres) en boucle.

Si vous souhaitez améliorer les performances de ClickHouse dans un certain scénario, et si ces améliorations peuvent être observées sur des requêtes simples, il est fortement recommandé d’écrire un test de performance.
Il est également recommandé d’écrire des tests de performance lorsque vous ajoutez ou modifiez des fonctions SQL relativement isolées et pas trop spécialisées.
Il est toujours utile d’utiliser `perf top` ou d’autres outils `perf` pendant vos tests.

<div id="test-tools-and-scripts">
  ## Outils et scripts de test
</div>

Certains programmes du répertoire `tests` ne sont pas des tests à proprement parler, mais des outils de test.
Par exemple, pour `Lexer`, il existe un outil `src/Parsers/tests/lexer` qui se contente d&#39;effectuer la tokenisation de stdin et d&#39;écrire le résultat colorisé sur stdout.
Vous pouvez utiliser ce type d&#39;outils comme exemples de code, ainsi que pour l&#39;exploration et les tests manuels.

<div id="miscellaneous-tests">
  ## Tests divers
</div>

Il existe des tests pour les modèles d’apprentissage automatique dans `tests/external_models`.
Ces tests ne sont plus mis à jour et doivent être transférés vers les tests d’intégration.

Il existe un test distinct pour les insertions avec quorum.
Ce test exécute un cluster ClickHouse sur des serveurs distincts et émule divers cas de défaillance : partitionnement du réseau, perte de paquets (entre les nœuds ClickHouse, entre ClickHouse et ZooKeeper, entre le serveur ClickHouse et le client, etc.), `kill -9`, `kill -STOP` et `kill -CONT`, comme [Jepsen](https://aphyr.com/tags/Jepsen). Le test vérifie ensuite que toutes les insertions dont la réception a été confirmée ont bien été écrites, et que toutes les insertions rejetées ne l’ont pas été.

<div id="manual-testing">
  ## Tests manuels
</div>

Lorsque vous développez une nouvelle fonctionnalité, il est logique de la tester également manuellement.
Vous pouvez procéder comme suit :

Compilez ClickHouse. Exécutez ClickHouse depuis le terminal : placez-vous dans le répertoire `programs/clickhouse-server`, puis lancez-le avec `./clickhouse-server`. Par défaut, il utilisera la configuration (`config.xml`, `users.xml` et les fichiers des répertoires `config.d` et `users.d`) du répertoire courant. Pour vous connecter au serveur ClickHouse, exécutez `programs/clickhouse-client/clickhouse-client`.

Notez que tous les outils clickhouse (serveur, client, etc.) ne sont en réalité que des liens symboliques vers un seul binaire nommé `clickhouse`.
Vous trouverez ce binaire dans `programs/clickhouse`.
Tous les outils peuvent également être appelés sous la forme `clickhouse tool` au lieu de `clickhouse-tool`.

Vous pouvez aussi installer le paquet ClickHouse : soit la release stable depuis le dépôt ClickHouse, soit compiler vous-même le paquet avec `./release` à la racine des sources ClickHouse.
Démarrez ensuite le serveur avec `sudo clickhouse start` (ou `stop` pour arrêter le serveur).
Consultez les logs dans `/etc/clickhouse-server/clickhouse-server.log`.

Si ClickHouse est déjà installé sur votre système, vous pouvez compiler un nouveau binaire `clickhouse` et remplacer le binaire existant :

```bash
$ sudo clickhouse stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo clickhouse start
```

Vous pouvez également arrêter le service système clickhouse-server et lancer votre propre instance avec la même configuration, mais avec la journalisation dans le terminal :

```bash
$ sudo clickhouse stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Exemple avec gdb :

```bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Si le système `clickhouse-server` est déjà en cours d’exécution et que vous ne souhaitez pas l’arrêter, vous pouvez modifier les numéros de port dans votre `config.xml` (ou les redéfinir dans un fichier du répertoire `config.d`), indiquer le chemin de données approprié, puis le lancer.

Le binaire `clickhouse` n’a pratiquement aucune dépendance et fonctionne sur un large éventail de distributions Linux.
Pour tester rapidement vos modifications sur un serveur à la va-vite, vous pouvez simplement copier via `scp` votre binaire `clickhouse` fraîchement compilé sur votre serveur, puis l’exécuter comme dans les exemples ci-dessus.

<div id="build-tests">
  ## Tests de compilation
</div>

Les tests de compilation permettent de vérifier que la compilation fonctionne correctement sur diverses configurations alternatives et sur certains systèmes moins courants.
Ces tests sont également automatisés.

Exemples :

* compilation croisée pour Darwin x86&#95;64 (macOS)
* compilation croisée pour FreeBSD x86&#95;64
* compilation croisée pour Linux AArch64
* compilation sur Ubuntu avec des bibliothèques issues des paquets système (déconseillé)
* compilation avec liaison dynamique des bibliothèques (déconseillé)

Par exemple, compiler avec des paquets système est une mauvaise pratique, car nous ne pouvons pas garantir la version exacte des paquets présents sur un système.
Mais c&#39;est réellement nécessaire pour les mainteneurs Debian.
Pour cette raison, nous devons au moins prendre en charge cette variante de compilation.
Autre exemple : la liaison dynamique est une source fréquente de problèmes, mais elle est nécessaire pour certains passionnés.

Même si nous ne pouvons pas exécuter tous les tests sur toutes les variantes de compilation, nous voulons au moins vérifier que les différentes variantes de compilation ne sont pas défaillantes.
À cette fin, nous utilisons des tests de compilation.

Nous vérifions également qu&#39;il n&#39;existe pas d&#39;unités de traduction trop longues à compiler ou nécessitant trop de RAM.

Nous vérifions également qu&#39;il n&#39;y a pas de trames de pile trop volumineuses.

<div id="testing-for-protocol-compatibility">
  ## Tests de compatibilité du protocole
</div>

Lorsque nous étendons le protocole réseau de ClickHouse, nous vérifions manuellement que l&#39;ancien clickhouse-client fonctionne avec le nouveau clickhouse-server, et que le nouveau clickhouse-client fonctionne avec l&#39;ancien clickhouse-server (simplement en exécutant les binaires des paquets correspondants).

Nous testons également automatiquement certains cas avec des tests d&#39;intégration :

* si les données écrites par une ancienne version de ClickHouse peuvent être lues correctement par la nouvelle version ;
* si les requêtes distribuées fonctionnent dans un cluster comportant différentes versions de ClickHouse.

<div id="help-from-the-compiler">
  ## Aide du compilateur
</div>

Le code principal de ClickHouse (situé dans le répertoire `src`) est compilé avec `-Wall -Wextra -Werror`, ainsi qu&#39;avec quelques avertissements supplémentaires activés.
En revanche, ces options ne sont pas activées pour les bibliothèques tierces.

Clang dispose d&#39;encore plus d&#39;avertissements utiles : vous pouvez les explorer avec `-Weverything` et en retenir certains pour la compilation par défaut.

Nous utilisons toujours clang pour compiler ClickHouse, aussi bien en développement qu&#39;en production.
Vous pouvez compiler sur votre propre machine en mode de débogage (pour économiser la batterie de votre ordinateur portable), mais notez que le compilateur peut générer davantage d&#39;avertissements avec `-O3`, grâce à une meilleure analyse du flux de contrôle et une meilleure analyse interprocédurale.
Lors d&#39;une compilation avec clang en mode de débogage, la version de débogage de `libc++` est utilisée, ce qui permet de détecter davantage d&#39;erreurs à l&#39;exécution.

<div id="sanitizers">
  ## Sanitizers
</div>

:::note
Si le processus (serveur ClickHouse ou client) plante au démarrage lorsque vous l’exécutez en local, vous devrez peut-être désactiver la randomisation de l’espace d’adressage : `sudo sysctl kernel.randomize_va_space=0`
:::

<div id="address-sanitizer">
  ### Address sanitizer
</div>

Nous exécutons des tests fonctionnels, d’intégration, de stress et unitaires sous ASan à chaque commit.

<div id="thread-sanitizer">
  ### Thread sanitizer
</div>

Nous exécutons les tests fonctionnels, d’intégration, de stress et unitaires sous TSan à chaque commit.

<div id="memory-sanitizer">
  ### Sanitizer de mémoire
</div>

Nous exécutons des tests fonctionnels, d’intégration, de stress et unitaires sous MSan à chaque commit.

<div id="undefined-behaviour-sanitizer">
  ### Sanitizer de comportements indéfinis
</div>

Nous exécutons des tests fonctionnels, d’intégration, de stress et unitaires sous UBSan à chaque commit.
Le code de certaines bibliothèques tierces n’est pas instrumenté pour détecter les comportements indéfinis.

<div id="valgrind-memcheck">
  ### Valgrind (memcheck)
</div>

Nous exécutions autrefois des tests fonctionnels avec Valgrind pendant la nuit, mais ce n’est plus le cas.
Cela prend plusieurs heures.
Il existe actuellement un faux positif connu dans la bibliothèque `re2`, voir [cet article](https://research.swtch.com/sparse).

<div id="fuzzing">
  ## Fuzzing
</div>

Le fuzzing de ClickHouse repose à la fois sur [libFuzzer](https://llvm.org/docs/LibFuzzer.html) et sur des requêtes SQL aléatoires.
Tous les tests de fuzzing doivent être exécutés avec des sanitizers (Address et Undefined).

LibFuzzer sert au fuzzing isolé du code des bibliothèques.
Les fuzzers sont implémentés dans le code de test et portent le suffixe &quot;&#95;fuzzer&quot;.
Vous trouverez un exemple de fuzzer dans `src/Parsers/fuzzers/lexer_fuzzer.cpp`.
Les configs, dictionnaires et corpus propres à LibFuzzer sont stockés dans `tests/fuzz`.
Nous vous encourageons à écrire des tests de fuzzing pour chaque fonctionnalité qui traite des entrées utilisateur.

Les fuzzers ne sont pas compilés par défaut.
Pour compiler les fuzzers, les options `-DENABLE_FUZZING=1` et `-DENABLE_TESTS=1` doivent toutes les deux être définies.
Nous recommandons de désactiver Jemalloc lors de la compilation des fuzzers.
La configuration utilisée pour intégrer le fuzzing de ClickHouse à
Google OSS-Fuzz se trouve dans `docker/fuzz`.

Nous utilisons également un test de fuzzing simple pour générer des requêtes SQL aléatoires et vérifier que le server ne s’arrête pas pendant leur exécution.
Vous le trouverez dans `00746_sql_fuzzy.pl`.
Ce test doit être exécuté en continu (toute la nuit, voire plus longtemps).

Nous utilisons aussi un fuzzer de requêtes sophistiqué, basé sur l’AST, capable de trouver un très grand nombre de cas limites.
Il effectue des permutations et des substitutions aléatoires dans l’AST des requêtes.
Il mémorise des nœuds AST issus de tests précédents pour les réutiliser lors du fuzzing des tests suivants, qu’il traite dans un ordre aléatoire.
Vous pouvez en apprendre davantage sur ce fuzzer dans [cet article de blog](https://clickhouse.com/blog/fuzzing-click-house).

<div id="stress-test">
  ## Test de stress
</div>

Les tests de stress sont une autre forme de fuzzing.
Ils exécutent tous les tests fonctionnels en parallèle, dans un ordre aléatoire, avec un seul serveur.
Les résultats des tests ne sont pas vérifiés.

Les points suivants sont vérifiés :

* le serveur ne plante pas, et aucun piège de débogage ni de sanitizer ne se déclenche ;
* il n’y a pas d’interblocages ;
* la structure de la base de données est cohérente ;
* le serveur peut s’arrêter correctement après le test et redémarrer sans exception.

Il existe cinq variantes (Débogage, ASan, TSan, MSan, UBSan).

<div id="thread-fuzzer">
  ## Thread fuzzer
</div>

Thread Fuzzer (à ne pas confondre avec Thread Sanitizer) est un autre type de fuzzing qui permet de rendre aléatoire l&#39;ordre d&#39;exécution des threads.
Il aide à trouver encore plus de cas particuliers.

<div id="security-audit">
  ## Audit de sécurité
</div>

Notre équipe de sécurité a effectué un examen général des fonctionnalités de ClickHouse du point de vue de la sécurité.

<div id="static-analyzers">
  ## Analyseurs statiques
</div>

Nous exécutons `clang-tidy` à chaque commit.
Les vérifications `clang-static-analyzer` sont également activées.
`clang-tidy` est aussi utilisé pour certaines vérifications de style.

Nous avons évalué `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`, `CodeQL`.
Vous trouverez les instructions d’utilisation dans le répertoire `tests/instructions/`.

Si vous utilisez `CLion` comme IDE, vous pouvez profiter de certaines vérifications `clang-tidy` prêtes à l’emploi.

Nous utilisons également `shellcheck` pour l’analyse statique des scripts shell.

<div id="hardening">
  ## Durcissement
</div>

Dans les builds de débogage, nous utilisons un allocator personnalisé qui applique l’ASLR aux allocations au niveau utilisateur.

Nous protégeons également manuellement les régions mémoire qui sont censées être en readonly après l’allocation.

Dans les builds de débogage, nous utilisons aussi une version personnalisée de la libc qui garantit qu’aucune fonction « nuisible » (Obsolete, non sécurisée, non thread-safe) n’est appelée.

Les assertions de débogage sont largement utilisées.

Dans les builds de débogage, si une exception avec le code « logical error » (ce qui implique un bogue) est levée, le programme s’arrête immédiatement.
Cela permet d’utiliser des exceptions dans les builds de release, tout en les traitant comme des assertions dans les builds de débogage.

La version de débogage de jemalloc est utilisée pour les builds de débogage.
La version de débogage de libc++ est utilisée pour les builds de débogage.

<div id="runtime-integrity-checks">
  ## Vérifications d&#39;intégrité à l&#39;exécution
</div>

Les données stockées sur disque sont protégées par des sommes de contrôle.
Les données des tables MergeTree sont protégées par des sommes de contrôle de trois manières simultanément* (blocs de données compressés, blocs de données non compressés, somme de contrôle globale sur l&#39;ensemble des blocs).
Les données transférées sur le réseau entre le client et le serveur, ou entre serveurs, sont également protégées par des sommes de contrôle.
La réplication garantit des données identiques bit à bit sur les répliques.

Cela est nécessaire pour se prémunir contre les défaillances matérielles (altération silencieuse des bits sur les supports de stockage, inversions de bits dans la RAM du serveur, inversions de bits dans la RAM du contrôleur réseau, inversions de bits dans la RAM du commutateur réseau, inversions de bits dans la RAM du client, inversions de bits dans le format binaire).
Notez que les inversions de bits sont fréquentes et peuvent se produire même avec de la RAM ECC et en présence de sommes de contrôle TCP (si vous faites tourner des milliers de serveurs traitant chacun des pétaoctets de données chaque jour).
[Voir la vidéo (russe)](https://www.youtube.com/watch?v=ooBAQIe0KlQ).

ClickHouse fournit des outils de diagnostic qui aideront les ingénieurs d&#39;exploitation à identifier le matériel défaillant.

* et ce n&#39;est pas lent.

<div id="code-style">
  ## Style de code
</div>

Les règles de style de code sont décrites [ici](style.md).

Pour détecter certaines violations courantes des règles de style, vous pouvez utiliser le script `utils/check-style`.

Pour imposer un style correct à votre code, vous pouvez utiliser `clang-format`.
Le fichier `.clang-format` se trouve à la racine des sources.
Il correspond dans l&#39;ensemble à notre style de code actuel.
Mais il n&#39;est pas recommandé d&#39;appliquer `clang-format` à des fichiers existants, car cela détériore la mise en forme.
Vous pouvez utiliser l&#39;outil `clang-format-diff`, que vous trouverez dans le dépôt source de clang.

Vous pouvez également essayer l&#39;outil `uncrustify` pour reformater votre code.
La configuration se trouve dans `uncrustify.cfg` à la racine des sources.
Il a été moins testé que `clang-format`.

`CLion` dispose de son propre formateur de code, qui doit être ajusté à notre style de code.

<div id="test-coverage">
  ## Couverture des tests
</div>

Nous suivons également la couverture des tests, mais uniquement pour les tests fonctionnels et pour clickhouse-server seulement.
Elle est mesurée quotidiennement.

<div id="tests-for-tests">
  ## Tests des tests
</div>

Une vérification automatisée permet de détecter les tests instables.
Elle exécute tous les nouveaux tests 100 fois (pour les tests fonctionnels) ou 10 fois (pour les tests d’intégration).
Si un test échoue ne serait-ce qu’une seule fois, il est considéré comme instable.

<div id="test-automation">
  ## Automatisation des tests
</div>

Nous exécutons les tests avec [GitHub Actions](https://github.com/features/actions).

Les jobs de build et les tests sont exécutés dans Sandbox pour chaque commit.
Les paquets générés et les résultats des tests sont publiés sur GitHub et peuvent être téléchargés via des liens directs.
Les artefacts sont conservés pendant plusieurs mois.
Lorsque vous envoyez une pull request sur GitHub, nous lui attribuons le libellé « can be tested » et notre système d’intégration continue compilera pour vous des paquets ClickHouse (release, debug, avec AddressSanitizer, etc.).