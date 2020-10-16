---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 76
toc_title: "S\xE9curit\xE9 Changelog"
---

## Correction dans la version 19.14.3.3 de ClickHouse, 2019-09-10 {#fixed-in-clickhouse-release-19-14-3-3-2019-09-10}

### CVE-2019-15024 {#cve-2019-15024}

Аn attacker that has write access to ZooKeeper and who ican run a custom server available from the network where ClickHouse runs, can create a custom-built malicious server that will act as a ClickHouse replica and register it in ZooKeeper. When another replica will fetch data part from the malicious replica, it can force clickhouse-server to write to arbitrary path on filesystem.

Crédits: Eldar Zaitov de L'équipe de sécurité de L'Information Yandex

### CVE-2019-16535 {#cve-2019-16535}

Аn OOB read, OOB write and integer underflow in decompression algorithms can be used to achieve RCE or DoS via native protocol.

Crédits: Eldar Zaitov de L'équipe de sécurité de L'Information Yandex

### CVE-2019-16536 {#cve-2019-16536}

Le débordement de pile menant à DoS peut être déclenché par un client authentifié malveillant.

Crédits: Eldar Zaitov de L'équipe de sécurité de L'Information Yandex

## Correction de la version 19.13.6.1 de ClickHouse, 2019-09-20 {#fixed-in-clickhouse-release-19-13-6-1-2019-09-20}

### CVE-2019-18657 {#cve-2019-18657}

Fonction de Table `url` la vulnérabilité avait-elle permis à l'attaquant d'injecter des en-têtes HTTP arbitraires dans la requête.

Crédit: [Nikita Tikhomirov](https://github.com/NSTikhomirov)

## Correction dans la version ClickHouse 18.12.13, 2018-09-10 {#fixed-in-clickhouse-release-18-12-13-2018-09-10}

### CVE-2018-14672 {#cve-2018-14672}

Les fonctions de chargement des modèles CatBoost permettaient de parcourir les chemins et de lire des fichiers arbitraires via des messages d'erreur.

Crédits: Andrey Krasichkov de L'équipe de sécurité de L'Information Yandex

## Correction dans la version 18.10.3 de ClickHouse, 2018-08-13 {#fixed-in-clickhouse-release-18-10-3-2018-08-13}

### CVE-2018-14671 {#cve-2018-14671}

unixODBC a permis de charger des objets partagés arbitraires à partir du système de fichiers, ce qui a conduit à une vulnérabilité D'exécution de Code À Distance.

Crédits: Andrey Krasichkov et Evgeny Sidorov de Yandex Information Security Team

## Correction dans la version 1.1.54388 de ClickHouse, 2018-06-28 {#fixed-in-clickhouse-release-1-1-54388-2018-06-28}

### CVE-2018-14668 {#cve-2018-14668}

“remote” la fonction de table a permis des symboles arbitraires dans “user”, “password” et “default\_database” champs qui ont conduit à des attaques de falsification de requêtes inter-protocoles.

Crédits: Andrey Krasichkov de L'équipe de sécurité de L'Information Yandex

## Correction dans la version 1.1.54390 de ClickHouse, 2018-07-06 {#fixed-in-clickhouse-release-1-1-54390-2018-07-06}

### CVE-2018-14669 {#cve-2018-14669}

Clickhouse client MySQL avait “LOAD DATA LOCAL INFILE” fonctionnalité activée permettant à une base de données MySQL malveillante de lire des fichiers arbitraires à partir du serveur clickhouse connecté.

Crédits: Andrey Krasichkov et Evgeny Sidorov de Yandex Information Security Team

## Correction dans la version 1.1.54131 de ClickHouse, 2017-01-10 {#fixed-in-clickhouse-release-1-1-54131-2017-01-10}

### CVE-2018-14670 {#cve-2018-14670}

Configuration incorrecte dans le paquet deb pourrait conduire à l'utilisation non autorisée de la base de données.

Crédits: National Cyber Security Centre (NCSC)

{## [Article Original](https://clickhouse.tech/docs/en/security_changelog/) ##}
