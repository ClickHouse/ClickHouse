---
description: 'Documentation de QUOTA'
sidebar_label: 'QUOTA'
sidebar_position: 42
slug: /sql-reference/statements/create/quota
title: 'CREATE QUOTA'
doc_type: 'reference'
---

Crée un [quota](../../../guides/sre/user-management/index.md#quotas-management) pouvant être attribué à un utilisateur ou à un rôle.

Syntaxe :

```sql
CREATE QUOTA [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | written_bytes | execution_time | failed_sequential_authentications | queries_per_normalized_hash} = number } [,...] |
         NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

Les clés `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address` et `normalized_query_hash` correspondent aux champs de la table [system.quotas](../../../operations/system-tables/quotas.md).

Les options `IPV4_PREFIX_BITS` et `IPV6_PREFIX_BITS` ne peuvent être utilisées que lorsque `KEYED BY` vaut `ip_address` ou `forwarded_ip_address`. Elles correspondent au champ de la table [system.quotas](../../../operations/system-tables/quotas.md).

Les paramètres `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `written_bytes`, `execution_time`, `failed_sequential_authentications`, `queries_per_normalized_hash` correspondent aux champs de la table [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md).

La clause `ON CLUSTER` permet de créer des quotas sur un cluster, voir [DDL distribué](../../../sql-reference/distributed-ddl.md).

**Exemples**

Limitez à 123 le nombre maximal de requêtes pour l’utilisateur courant dans une contrainte de 15 mois :

```sql
CREATE QUOTA qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

Pour l’utilisateur `default`, limitez le temps d’exécution maximal à une demi-seconde sur une période de 30 minutes, et limitez le nombre maximal de requêtes à 321 ainsi que le nombre maximal d’erreurs à 10 sur 5 quarts d’heure :

```sql
CREATE QUOTA qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```

Créez un quota dans lequel chaque modèle de requête normalisé distinct dispose de son propre bucket, avec une limite de 100 exécutions par heure :

```sql
CREATE QUOTA qC KEYED BY normalized_query_hash FOR INTERVAL 1 hour MAX queries = 100 TO default;
```

Limitez tout modèle de requête normalisée à un maximum de 50 exécutions par heure (indépendamment du type de clé de quota) :

```sql
CREATE QUOTA qD FOR INTERVAL 1 hour MAX queries_per_normalized_hash = 50 TO default;
```

D’autres exemples, avec la configuration XML (non prise en charge dans ClickHouse Cloud), sont disponibles dans le [guide des quotas](/fr/operations/quotas).

<div id="related-content">
  ## Contenu associé
</div>

* Blog : [Créer des applications monopage avec ClickHouse](https://clickhouse.com/blog/building-single-page-applications-with-clickhouse-and-http)