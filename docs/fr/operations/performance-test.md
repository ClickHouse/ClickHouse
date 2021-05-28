---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "Tester Le Mat\xE9riel"
---

# Comment tester votre matériel avec ClickHouse {#how-to-test-your-hardware-with-clickhouse}

Avec cette instruction, vous pouvez exécuter le test de performance clickhouse de base sur n'importe quel serveur sans installation de paquets ClickHouse.

1.  Aller à “commits” page: https://github.com/ClickHouse/ClickHouse/commits/master

2.  Cliquez sur la première coche verte ou croix rouge avec vert “ClickHouse Build Check” et cliquez sur le “Details” lien de proximité “ClickHouse Build Check”. Il n'y a pas un tel lien dans certains commits, par exemple des commits avec de la documentation. Dans ce cas, choisissez le commit le plus proche ayant ce lien.

3.  Copiez le lien à “clickhouse” binaire pour amd64 ou aarch64.

4.  ssh sur le serveur et le télécharger avec wget:

<!-- -->

      # For amd64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
      # For aarch64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
      # Then do:
      chmod a+x clickhouse

1.  Télécharger configs:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/users.xml
      mkdir config.d
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/path.xml -O config.d/path.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml

1.  Télécharger des fichiers de référence:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
      chmod a+x benchmark-new.sh
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql

1.  Télécharger les données de test selon le [Yandex.Metrica dataset](../getting-started/example-datasets/metrica.md) instruction (“hits” tableau contenant 100 millions de lignes).

<!-- -->

      wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
      tar xvf hits_100m_obfuscated_v1.tar.xz -C .
      mv hits_100m_obfuscated_v1/* .

1.  Exécuter le serveur:

<!-- -->

      ./clickhouse server

1.  Vérifiez les données: ssh au serveur dans un autre terminal

<!-- -->

      ./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
      100000000

1.  Modifier le benchmark-new.sh, changement `clickhouse-client` de `./clickhouse client` et d'ajouter `–-max_memory_usage 100000000000` paramètre.

<!-- -->

      mcedit benchmark-new.sh

1.  Exécutez le test:

<!-- -->

      ./benchmark-new.sh hits_100m_obfuscated

1.  Envoyez les numéros et les informations sur votre configuration matérielle à clickhouse-feedback@yandex-team.com

Tous les résultats sont publiés ici: https://clickhouse.tech/de référence/de matériel/
