---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 69
toc_title: "Comment ex\xE9cuter des Tests ClickHouse"
---

# ClickHouse Test {#clickhouse-testing}

## Les Tests Fonctionnels {#functional-tests}

Les tests fonctionnels sont les plus simples et pratiques à utiliser. La plupart des fonctionnalités de ClickHouse peuvent être testées avec des tests fonctionnels et elles sont obligatoires à utiliser pour chaque changement de code de ClickHouse qui peut être testé de cette façon.

Chaque test fonctionnel envoie une ou plusieurs requêtes au serveur clickhouse en cours d'exécution et compare le résultat avec la référence.

Les Tests sont situés dans `queries` répertoire. Il y a deux sous-répertoires: `stateless` et `stateful`. Les tests sans état exécutent des requêtes sans données de test préchargées - ils créent souvent de petits ensembles de données synthétiques à la volée, dans le test lui-même. Les tests avec État nécessitent des données de test préchargées de Yandex.Metrica et non disponible pour le grand public. Nous avons tendance à utiliser uniquement `stateless` tests et éviter d'ajouter de nouveaux `stateful` test.

Chaque test peut être de deux types: `.sql` et `.sh`. `.sql` test est le script SQL simple qui est canalisé vers `clickhouse-client --multiquery --testmode`. `.sh` test est un script qui est exécuté par lui-même.

Pour exécuter tous les tests, utilisez `clickhouse-test` outil. Regarder `--help` pour la liste des options possibles. Vous pouvez simplement exécuter tous les tests ou exécuter un sous ensemble de tests filtrés par sous chaîne dans le nom du test: `./clickhouse-test substring`.

Le moyen le plus simple d'invoquer des tests fonctionnels est de copier `clickhouse-client` de `/usr/bin/`, exécuter `clickhouse-server` et puis exécutez `./clickhouse-test` à partir de son propre répertoire.

Pour ajouter un nouveau test, créez un `.sql` ou `.sh` fichier dans `queries/0_stateless` répertoire, vérifiez-le manuellement, puis générez `.reference` fichier de la façon suivante: `clickhouse-client -n --testmode < 00000_test.sql > 00000_test.reference` ou `./00000_test.sh > ./00000_test.reference`.

Les Tests doivent utiliser (create, drop, etc) uniquement des tables dans `test` base de données supposée être créée au préalable; les tests peuvent également utiliser des tables temporaires.

Si vous souhaitez utiliser des requêtes distribuées dans les tests fonctionnels, vous pouvez tirer parti de `remote` fonction de table avec `127.0.0.{1..2}` ou vous pouvez utiliser des clusters de test prédéfinis dans le fichier de configuration du serveur comme `test_shard_localhost`.

Certains tests sont marqués avec `zookeeper`, `shard` ou `long` en leurs noms.
`zookeeper` est pour les tests qui utilisent ZooKeeper. `shard` est pour les tests
nécessite l'écoute du serveur `127.0.0.*`; `distributed` ou `global` avoir le même
sens. `long` est pour les tests qui s'exécutent légèrement plus longtemps qu'une seconde. Vous pouvez
désactivez ces groupes de tests en utilisant `--no-zookeeper`, `--no-shard` et
`--no-long` options, respectivement.

## Bugs Connus {#known-bugs}

Si nous connaissons des bugs qui peuvent être facilement reproduits par des tests fonctionnels, nous plaçons des tests fonctionnels préparés dans `tests/queries/bugs` répertoire. Ces tests seront déplacés à `tests/queries/0_stateless` quand les bugs sont corrigés.

## Les Tests D'Intégration {#integration-tests}

Les tests d'intégration permettent de tester ClickHouse en configuration cluster et clickhouse interaction avec D'autres serveurs comme MySQL, Postgres, MongoDB. Ils sont utiles pour émuler les splits réseau, les chutes de paquets, etc. Ces tests sont exécutés sous Docker et créent plusieurs conteneurs avec divers logiciels.

Voir `tests/integration/README.md` sur la façon d'exécuter ces tests.

Notez que l'intégration de ClickHouse avec des pilotes tiers n'est pas testée. De plus, nous n'avons actuellement pas de tests d'intégration avec nos pilotes JDBC et ODBC.

## Les Tests Unitaires {#unit-tests}

Les tests unitaires sont utiles lorsque vous voulez tester non pas le ClickHouse dans son ensemble, mais une seule bibliothèque ou classe isolée. Vous pouvez activer ou désactiver la génération de tests avec `ENABLE_TESTS` Option CMake. Les tests unitaires (et autres programmes de test) sont situés dans `tests` sous-répertoires à travers le code. Pour exécuter des tests unitaires, tapez `ninja test`. Certains tests utilisent `gtest`, mais certains ne sont que des programmes qui renvoient un code de sortie non nul en cas d'échec du test.

Ce n'est pas nécessairement d'avoir des tests unitaires si le code est déjà couvert par des tests fonctionnels (et les tests fonctionnels sont généralement beaucoup plus simples à utiliser).

## Tests De Performance {#performance-tests}

Les tests de Performance permettent de mesurer et de comparer les performances d'une partie isolée de ClickHouse sur des requêtes synthétiques. Les Tests sont situés à `tests/performance`. Chaque test est représenté par `.xml` fichier avec description du cas de test. Les Tests sont exécutés avec `clickhouse performance-test` outil (qui est incorporé dans `clickhouse` binaire). Voir `--help` pour l'invocation.

Chaque essai d'exécuter une ou plusieurs requêtes (éventuellement avec des combinaisons de paramètres) dans une boucle avec certaines conditions pour l'arrêt (comme “maximum execution speed is not changing in three seconds”) et mesurer certaines mesures sur les performances de la requête (comme “maximum execution speed”). Certains tests peuvent contenir des conditions préalables sur un ensemble de données de test préchargé.

Si vous souhaitez améliorer les performances de ClickHouse dans certains scénarios, et si des améliorations peuvent être observées sur des requêtes simples, il est fortement recommandé d'écrire un test de performance. Il est toujours logique d'utiliser `perf top` ou d'autres outils perf pendant vos tests.

## Outils et Scripts de Test {#test-tools-and-scripts}

Certains programmes dans `tests` directory ne sont pas des tests préparés, mais sont des outils de test. Par exemple, pour `Lexer` il est un outil `src/Parsers/tests/lexer` Cela fait juste la tokenisation de stdin et écrit le résultat colorisé dans stdout. Vous pouvez utiliser ce genre d'outils comme exemples de code et pour l'exploration et les tests manuels.

Vous pouvez également placer une paire de fichiers `.sh` et `.reference` avec l'outil pour l'exécuter sur une entrée prédéfinie - alors le résultat du script peut être comparé à `.reference` fichier. Ce genre de tests ne sont pas automatisés.

## Divers Tests {#miscellaneous-tests}

Il existe des tests pour les dictionnaires externes situés à `tests/external_dictionaries` et pour machine appris modèles dans `tests/external_models`. Ces tests ne sont pas mis à jour et doivent être transférés aux tests d'intégration.

Il y a un test séparé pour les inserts de quorum. Ce test exécute le cluster ClickHouse sur des serveurs séparés et émule divers cas d'échec: scission réseau, chute de paquets (entre les nœuds ClickHouse, entre Clickhouse et ZooKeeper, entre le serveur ClickHouse et le client, etc.), `kill -9`, `kill -STOP` et `kill -CONT` , comme [Jepsen](https://aphyr.com/tags/Jepsen). Ensuite, le test vérifie que toutes les insertions reconnues ont été écrites et que toutes les insertions rejetées ne l'ont pas été.

Le test de Quorum a été écrit par une équipe distincte avant que ClickHouse ne soit open-source. Cette équipe ne travaille plus avec ClickHouse. Test a été écrit accidentellement en Java. Pour ces raisons, quorum test doit être réécrit et déplacé vers tests d'intégration.

## Les Tests Manuels {#manual-testing}

Lorsque vous développez une nouvelle fonctionnalité, il est raisonnable de tester également manuellement. Vous pouvez le faire avec les étapes suivantes:

Construire ClickHouse. Exécuter ClickHouse à partir du terminal: changer le répertoire à `programs/clickhouse-server` et de l'exécuter avec `./clickhouse-server`. Il utilisera la configuration (`config.xml`, `users.xml` et les fichiers à l'intérieur `config.d` et `users.d` répertoires) à partir du répertoire courant par défaut. Pour vous connecter au serveur ClickHouse, exécutez `programs/clickhouse-client/clickhouse-client`.

Notez que tous les outils clickhouse (serveur, client, etc.) ne sont que des liens symboliques vers un seul binaire nommé `clickhouse`. Vous pouvez trouver ce binaire à `programs/clickhouse`. Tous les outils peuvent également être invoquée comme `clickhouse tool` plutôt `clickhouse-tool`.

Alternativement, vous pouvez installer le paquet ClickHouse: soit une version stable du référentiel Yandex, soit vous pouvez créer un paquet pour vous-même avec `./release` dans les sources de ClickHouse racine. Puis démarrez le serveur avec `sudo service clickhouse-server start` (ou stop pour arrêter le serveur). Rechercher des journaux à `/etc/clickhouse-server/clickhouse-server.log`.

Lorsque ClickHouse est déjà installé sur votre système, vous pouvez créer un nouveau `clickhouse` binaire et remplacer le binaire:

``` bash
$ sudo service clickhouse-server stop
$ sudo cp ./clickhouse /usr/bin/
$ sudo service clickhouse-server start
```

Vous pouvez également arrêter system clickhouse-server et exécuter le vôtre avec la même configuration mais en vous connectant au terminal:

``` bash
$ sudo service clickhouse-server stop
$ sudo -u clickhouse /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Exemple avec gdb:

``` bash
$ sudo -u clickhouse gdb --args /usr/bin/clickhouse server --config-file /etc/clickhouse-server/config.xml
```

Si le système clickhouse-server est déjà en cours d'exécution et que vous ne voulez pas l'arrêter, vous pouvez modifier les numéros de port dans votre `config.xml` (ou de les remplacer dans un fichier `config.d` répertoire), fournissez le chemin de données approprié, et exécutez-le.

`clickhouse` binary n'a presque aucune dépendance et fonctionne sur un large éventail de distributions Linux. Rapide et sale de tester vos modifications sur un serveur, vous pouvez simplement `scp` votre douce construite `clickhouse` binaire à votre serveur et ensuite l'exécuter comme dans les exemples ci-dessus.

## L'Environnement De Test {#testing-environment}

Avant de publier la version stable, nous la déployons sur l'environnement de test. L'environnement de test est un cluster processus 1/39 partie de [Yandex.Metrica](https://metrica.yandex.com/) données. Nous partageons notre environnement de test avec Yandex.Metrica de l'équipe. ClickHouse est mis à niveau sans temps d'arrêt au-dessus des données existantes. Nous regardons d'abord que les données sont traitées avec succès sans retard par rapport au temps réel, la réplication continue à fonctionner et il n'y a pas de problèmes visibles pour Yandex.Metrica de l'équipe. Première vérification peut être effectuée de la façon suivante:

``` sql
SELECT hostName() AS h, any(version()), any(uptime()), max(UTCEventTime), count() FROM remote('example01-01-{1..3}t', merge, hits) WHERE EventDate >= today() - 2 GROUP BY h ORDER BY h;
```

Dans certains cas, nous déployons également à l'environnement de test de nos équipes d'amis dans Yandex: marché, Cloud, etc. Nous avons également des serveurs matériels qui sont utilisés à des fins de développement.

## Les Tests De Charge {#load-testing}

Après le déploiement dans l'environnement de test, nous exécutons des tests de charge avec des requêtes du cluster de production. Ceci est fait manuellement.

Assurez-vous que vous avez activé `query_log` sur votre cluster de production.

Recueillir le journal des requêtes pour une journée ou plus:

``` bash
$ clickhouse-client --query="SELECT DISTINCT query FROM system.query_log WHERE event_date = today() AND query LIKE '%ym:%' AND query NOT LIKE '%system.query_log%' AND type = 2 AND is_initial_query" > queries.tsv
```

C'est une façon compliquée exemple. `type = 2` filtrera les requêtes exécutées avec succès. `query LIKE '%ym:%'` est de sélectionner les requêtes de Yandex.Metrica. `is_initial_query` est de sélectionner uniquement les requêtes initiées par le client, pas par ClickHouse lui-même (en tant que partie du traitement de requête distribué).

`scp` ce journal à votre cluster de test et l'exécuter comme suit:

``` bash
$ clickhouse benchmark --concurrency 16 < queries.tsv
```

(probablement vous voulez aussi spécifier un `--user`)

Ensuite, laissez-le pour une nuit ou un week-end et allez vous reposer.

Tu devrais vérifier ça `clickhouse-server` ne plante pas, l'empreinte mémoire est limitée et les performances ne se dégradent pas au fil du temps.

Les délais précis d'exécution des requêtes ne sont pas enregistrés et ne sont pas comparés en raison de la grande variabilité des requêtes et de l'environnement.

## Essais De Construction {#build-tests}

Les tests de construction permettent de vérifier que la construction n'est pas interrompue sur diverses configurations alternatives et sur certains systèmes étrangers. Les Tests sont situés à `ci` répertoire. Ils exécutent build from source à L'intérieur de Docker, Vagrant, et parfois avec `qemu-user-static` à l'intérieur de Docker. Ces tests sont en cours de développement et les essais ne sont pas automatisées.

Motivation:

Normalement, nous libérons et exécutons tous les tests sur une seule variante de construction ClickHouse. Mais il existe des variantes de construction alternatives qui ne sont pas complètement testées. Exemple:

-   construire sur FreeBSD;
-   construire sur Debian avec les bibliothèques des paquets système;
-   construire avec des liens partagés de bibliothèques;
-   construire sur la plate-forme AArch64;
-   construire sur la plate-forme PowerPc.

Par exemple, construire avec des paquets système est une mauvaise pratique, car nous ne pouvons pas garantir quelle version exacte des paquets un système aura. Mais c'est vraiment nécessaire pour les responsables Debian. Pour cette raison, nous devons au moins soutenir cette variante de construction. Un autre exemple: la liaison partagée est une source commune de problèmes, mais elle est nécessaire pour certains amateurs.

Bien que nous ne puissions pas exécuter tous les tests sur toutes les variantes de builds, nous voulons vérifier au moins que les différentes variantes de build ne sont pas cassées. Pour cela nous utilisons les essais de construction.

## Test de compatibilité du protocole {#testing-for-protocol-compatibility}

Lorsque nous étendons le protocole réseau ClickHouse, nous testons manuellement que l'ancien clickhouse-client fonctionne avec le nouveau clickhouse-server et que le nouveau clickhouse-client fonctionne avec l'ancien clickhouse-server (simplement en exécutant des binaires à partir des paquets correspondants).

## L'aide du Compilateur {#help-from-the-compiler}

Code ClickHouse principal (qui est situé dans `dbms` annuaire) est construit avec `-Wall -Wextra -Werror` et avec quelques avertissements supplémentaires activés. Bien que ces options ne soient pas activées pour les bibliothèques tierces.

Clang a des avertissements encore plus utiles - vous pouvez les chercher avec `-Weverything` et choisissez quelque chose à construire par défaut.

Pour les builds de production, gcc est utilisé (il génère toujours un code légèrement plus efficace que clang). Pour le développement, clang est généralement plus pratique à utiliser. Vous pouvez construire sur votre propre machine avec le mode débogage (pour économiser la batterie de votre ordinateur portable), mais veuillez noter que le compilateur est capable de générer plus d'Avertissements avec `-O3` grâce à une meilleure analyse du flux de contrôle et de l'inter-procédure. Lors de la construction avec clang, `libc++` est utilisé au lieu de `libstdc++` et lors de la construction avec le mode débogage, la version de débogage de `libc++` est utilisé qui permet d'attraper plus d'erreurs à l'exécution.

## Désinfectant {#sanitizers}

**Désinfectant d'adresse**.
Nous exécutons des tests fonctionnels et d'intégration sous ASan sur la base de per-commit.

**Valgrind (Memcheck)**.
Nous effectuons des tests fonctionnels sous Valgrind pendant la nuit. Cela prend plusieurs heures. Actuellement il y a un faux positif connu dans `re2` bibliothèque, consultez [cet article](https://research.swtch.com/sparse).

**Désinfectant de comportement indéfini.**
Nous exécutons des tests fonctionnels et d'intégration sous ASan sur la base de per-commit.

**Désinfectant pour filetage**.
Nous exécutons des tests fonctionnels sous TSan sur la base de per-commit. Nous n'exécutons toujours pas de tests D'intégration sous TSan sur la base de la validation.

**Mémoire de désinfectant**.
Actuellement, nous n'utilisons toujours pas MSan.

**Débogueur allocateur.**
Version de débogage de `jemalloc` est utilisé pour la construction de débogage.

## Fuzzing {#fuzzing}

Clickhouse fuzzing est implémenté à la fois en utilisant [libFuzzer](https://llvm.org/docs/LibFuzzer.html) et des requêtes SQL aléatoires.
Tous les tests de fuzz doivent être effectués avec des désinfectants (adresse et indéfini).

LibFuzzer est utilisé pour les tests de fuzz isolés du code de la bibliothèque. Les Fuzzers sont implémentés dans le cadre du code de test et ont “\_fuzzer” nom postfixes.
Exemple Fuzzer peut être trouvé à `src/Parsers/tests/lexer_fuzzer.cpp`. Les configs, dictionnaires et corpus spécifiques à LibFuzzer sont stockés à `tests/fuzz`.
Nous vous encourageons à écrire des tests fuzz pour chaque fonctionnalité qui gère l'entrée de l'utilisateur.

Fuzzers ne sont pas construits par défaut. Pour construire fuzzers à la fois `-DENABLE_FUZZING=1` et `-DENABLE_TESTS=1` options doivent être définies.
Nous vous recommandons de désactiver Jemalloc lors de la construction de fuzzers. Configuration utilisée pour intégrer clickhouse fuzzing à
Google OSS-Fuzz peut être trouvé à `docker/fuzz`.

Nous utilisons également un simple test fuzz pour générer des requêtes SQL aléatoires et vérifier que le serveur ne meurt pas en les exécutant.
Vous pouvez le trouver dans `00746_sql_fuzzy.pl`. Ce test doit être exécuté en continu (pendant la nuit et plus longtemps).

## Audit De Sécurité {#security-audit}

Les gens de L'équipe de sécurité Yandex font un aperçu de base des capacités de ClickHouse du point de vue de la sécurité.

## Analyseurs Statiques {#static-analyzers}

Nous courons `PVS-Studio` par commettre base. Nous avons évalué `clang-tidy`, `Coverity`, `cppcheck`, `PVS-Studio`, `tscancode`. Vous trouverez des instructions pour l'utilisation dans `tests/instructions/` répertoire. Aussi, vous pouvez lire [l'article en russe](https://habr.com/company/yandex/blog/342018/).

Si vous utilisez `CLion` en tant QU'IDE, vous pouvez tirer parti de certains `clang-tidy` contrôles de la boîte.

## Durcir {#hardening}

`FORTIFY_SOURCE` est utilisé par défaut. C'est presque inutile, mais cela a toujours du sens dans de rares cas et nous ne le désactivons pas.

## Code De Style {#code-style}

Les règles de style de Code sont décrites [ici](https://clickhouse.tech/docs/en/development/style/).

Pour vérifier certaines violations de style courantes, vous pouvez utiliser `utils/check-style` script.

Pour forcer le style approprié de votre code, vous pouvez utiliser `clang-format`. Fichier `.clang-format` est situé à la racine des sources. Il correspond principalement à notre style de code réel. Mais il n'est pas recommandé d'appliquer `clang-format` pour les fichiers existants, car il rend le formatage pire. Vous pouvez utiliser `clang-format-diff` outil que vous pouvez trouver dans clang référentiel source.

Alternativement vous pouvez essayer `uncrustify` outil pour reformater votre code. La Configuration est en `uncrustify.cfg` dans la racine des sources. Il est moins testé que `clang-format`.

`CLion` a son propre formateur de code qui doit être réglé pour notre style de code.

## Tests Metrica B2B {#metrica-b2b-tests}

Chaque version de ClickHouse est testée avec les moteurs Yandex Metrica et AppMetrica. Les versions de test et stables de ClickHouse sont déployées sur des machines virtuelles et exécutées avec une petite copie de metrica engine qui traite un échantillon fixe de données d'entrée. Ensuite, les résultats de deux instances de metrica engine sont comparés ensemble.

Ces tests sont automatisés par une équipe distincte. En raison du nombre élevé de pièces en mouvement, les tests échouent la plupart du temps complètement raisons, qui sont très difficiles à comprendre. Très probablement, ces tests ont une valeur négative pour nous. Néanmoins, ces tests se sont révélés utiles dans environ une ou deux fois sur des centaines.

## La Couverture De Test {#test-coverage}

En juillet 2018, nous ne suivons pas la couverture des tests.

## Automatisation Des Tests {#test-automation}

Nous exécutons des tests avec Yandex CI interne et le système d'automatisation des tâches nommé “Sandbox”.

Les travaux de construction et les tests sont exécutés dans Sandbox sur une base de validation. Les paquets résultants et les résultats des tests sont publiés dans GitHub et peuvent être téléchargés par des liens directs. Les artefacts sont stockés éternellement. Lorsque vous envoyez une demande de tirage sur GitHub, nous l'étiquetons comme “can be tested” et notre système CI construira des paquets ClickHouse (release, debug, avec un désinfectant d'adresse, etc.) pour vous.

Nous n'utilisons pas Travis CI en raison de la limite de temps et de puissance de calcul.
On n'utilise pas Jenkins. Il a été utilisé avant et maintenant nous sommes heureux de ne pas utiliser Jenkins.

[Article Original](https://clickhouse.tech/docs/en/development/tests/) <!--hide-->
