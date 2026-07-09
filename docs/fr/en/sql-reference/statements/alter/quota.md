---
description: 'Documentation sur QUOTA'
sidebar_label: 'QUOTA'
sidebar_position: 46
slug: /sql-reference/statements/alter/quota
title: 'ALTER QUOTA'
doc_type: 'reference'
---

Modifie les quotas.

Syntaxe :

```sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {user_name | ip_address | forwarded_ip_address | client_key | client_key,user_name | client_key,ip_address | normalized_query_hash} | NOT KEYED]
    [IPV4_PREFIX_BITS number]
    [IPV6_PREFIX_BITS number]
    [FOR [RANDOMIZED] INTERVAL number {second | minute | hour | day | week | month | quarter | year}
        {MAX { {queries | query_selects | query_inserts | errors | result_rows | result_bytes | read_rows | read_bytes | execution_time | queries_per_normalized_hash} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

Les clés `user_name`, `ip_address`, `forwarded_ip_address`, `client_key`, `client_key, user_name`, `client_key, ip_address` et `normalized_query_hash` correspondent aux champs de la table [system.quotas](../../../operations/system-tables/quotas.md).

Les options `IPV4_PREFIX_BITS` et `IPV6_PREFIX_BITS` ne peuvent être utilisées que lorsque `KEYED BY` vaut `ip_address` ou `forwarded_ip_address`. Elles correspondent au champ de la table [system.quotas](../../../operations/system-tables/quotas.md).

Les paramètres `queries`, `query_selects`, `query_inserts`, `errors`, `result_rows`, `result_bytes`, `read_rows`, `read_bytes`, `execution_time`, `queries_per_normalized_hash` correspondent aux champs de la table [system.quotas&#95;usage](../../../operations/system-tables/quotas_usage.md).

La clause `ON CLUSTER` permet de créer des quotas sur un cluster, voir [DDL distribué](../../../sql-reference/distributed-ddl.md).

**Exemples**

Limitez le nombre maximal de requêtes de l’utilisateur courant à 123 requêtes sur une période de 15 mois :

```sql
ALTER QUOTA IF EXISTS qA FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER;
```

Pour l’utilisateur `default`, limitez le temps d’exécution maximal à une demi-seconde sur une période de 30 minutes, ainsi que le nombre maximal de requêtes à 321 et le nombre maximal d’erreurs à 10 sur une période de 5 trimestres :

```sql
ALTER QUOTA IF EXISTS qB FOR INTERVAL 30 minute MAX execution_time = 0.5, FOR INTERVAL 5 quarter MAX queries = 321, errors = 10 TO default;
```