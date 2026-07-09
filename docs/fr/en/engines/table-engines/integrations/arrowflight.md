---
description: 'Le moteur permet d’exécuter des requêtes sur des jeux de données distants et d’y insérer des données via Apache Arrow Flight.'
sidebar_label: 'ArrowFlight'
sidebar_position: 186
slug: /engines/table-engines/integrations/arrowflight
title: 'Moteur de table ArrowFlight'
doc_type: 'reference'
---

Le moteur de table ArrowFlight permet à ClickHouse de lire des jeux de données distants et d’y écrire via le protocole [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html).
Cette intégration permet à ClickHouse d’interagir avec des serveurs Flight externes au format Arrow colonnaire, avec des performances élevées.

<div id="creating-a-table">
  ## Créer une table
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name (name1 [type1], name2 [type2], ...)
    ENGINE = ArrowFlight('host:port', 'dataset_name' [, 'username', 'password']);
```

**Paramètres du moteur**

* `host:port` — Adresse du serveur Arrow Flight distant. Si le port est omis, le port par défaut `8815` est utilisé. [String](../../../sql-reference/data-types/string.md).
* `dataset_name` — Identifiant du jeu de données sur le serveur Flight (utilisé comme descripteur PATH ou dans une requête `SELECT *` selon le paramètre `arrow_flight_request_descriptor_type`). [String](../../../sql-reference/data-types/string.md).
* `username` — Nom d’utilisateur pour l’authentification HTTP Basic. [String](../../../sql-reference/data-types/string.md).
* `password` — Mot de passe pour l’authentification HTTP Basic. [String](../../../sql-reference/data-types/string.md).

Si `username` et `password` sont omis, l’authentification n’est pas utilisée (cela fonctionne uniquement si le serveur Arrow Flight autorise l’accès sans authentification).

La liste des colonnes est facultative — si elle est omise, le schéma est inféré à partir du serveur Arrow Flight distant via `GetSchema`.

<div id="named-collections">
  ## Collections nommées
</div>

Le moteur prend en charge les [collections nommées](/fr/operations/named-collections) pour stocker les paramètres de connexion :

```sql
CREATE TABLE remote_flight_data
    ENGINE = ArrowFlight(named_collection_name);
```

Paramètres de la collection nommée :

| Paramètre                  | Obligatoire     | Par défaut | Description                                                       |
| -------------------------- | --------------- | ---------- | ----------------------------------------------------------------- |
| `host` or `hostname`       | Non             | `""`       | Nom d’hôte du serveur.                                            |
| `port`                     | Oui             | —          | Port du serveur.                                                  |
| `dataset`                  | Non             | `""`       | Nom du jeu de données ou descripteur.                                    |
| `use_basic_authentication` | Non             | `true`     | Active l’authentification de base.                                |
| `user` or `username`       | Si auth activée | —          | Nom d’utilisateur pour l’authentification.                        |
| `password`                 | Non             | `""`       | Mot de passe pour l’authentification.                             |
| `enable_ssl`               | Non             | `false`    | Active le chiffrement TLS.                                        |
| `ssl_ca`                   | Non             | `""`       | Chemin vers le fichier de certificat CA pour la vérification TLS. |
| `ssl_override_hostname`    | Non             | `""`       | Remplace le nom d’hôte vérifié lors de la vérification TLS.       |

<div id="settings">
  ## Paramètres
</div>

* `arrow_flight_request_descriptor_type` — Contrôle la manière dont le nom du jeu de données est transmis au serveur Flight. Valeurs possibles : `path` (par défaut, envoyé comme descripteur PATH) ou `command` (envoyé comme descripteur CMD avec `SELECT * FROM <dataset>`). Utilisez `command` pour les serveurs Flight qui attendent des commandes SQL (par exemple, Dremio).

<div id="usage-example">
  ## Exemple d&#39;utilisation
</div>

Lecture de données à partir d’un serveur Arrow Flight distant :

```sql
CREATE TABLE remote_flight_data
(
    id UInt32,
    name String,
    value Float64
) ENGINE = ArrowFlight('127.0.0.1:9005', 'sample_dataset');

SELECT * FROM remote_flight_data ORDER BY id;
```

```text
┌─id─┬─name────┬─value─┐
│  1 │ foo     │ 42.1  │
│  2 │ bar     │ 13.3  │
│  3 │ baz     │ 77.0  │
└────┴─────────┴───────┘
```

Insertion de données vers un serveur Arrow Flight distant :

```sql
INSERT INTO remote_flight_data VALUES (4, 'qux', 99.9);
```

<div id="notes">
  ## Remarques
</div>

* Si des colonnes sont spécifiées dans l’instruction `CREATE TABLE`, elles doivent correspondre au schéma renvoyé par le serveur Flight.
* Si les colonnes sont omises, le schéma est inféré automatiquement depuis le serveur distant.
* La lecture (`SELECT`) comme l’écriture (`INSERT`) sont prises en charge.
* Le paramètre `arrow_flight_request_descriptor_type` détermine si le nom du jeu de données est envoyé sous forme de descripteur PATH ou de descripteur CMD encapsulant une requête `SELECT *`.

<div id="see-also">
  ## Voir aussi
</div>

* [fonction de table arrowFlight](/fr/sql-reference/table-functions/arrowflight)
* [interface Arrow Flight](/fr/interfaces/arrowflight)
* [spécification Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
* [format Arrow dans ClickHouse](/fr/interfaces/formats/Arrow)