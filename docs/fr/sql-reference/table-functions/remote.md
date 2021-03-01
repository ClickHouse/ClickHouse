---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: distant
---

# à distance, remoteSecure {#remote-remotesecure}

Vous permet d'accéder à des serveurs distants sans `Distributed` table.

Signature:

``` sql
remote('addresses_expr', db, table[, 'user'[, 'password']])
remote('addresses_expr', db.table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db, table[, 'user'[, 'password']])
remoteSecure('addresses_expr', db.table[, 'user'[, 'password']])
```

`addresses_expr` – An expression that generates addresses of remote servers. This may be just one server address. The server address is `host:port` ou juste `host`. L'hôte peut être spécifié comme nom de serveur ou l'adresse IPv4 ou IPv6. Une adresse IPv6 est indiquée entre crochets. Le port est le port TCP sur le serveur distant. Si le port est omis, il utilise `tcp_port` à partir du fichier de configuration du serveur (par défaut, 9000).

!!! important "Important"
    Le port est requis pour une adresse IPv6.

Exemple:

``` text
example01-01-1
example01-01-1:9000
localhost
127.0.0.1
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Plusieurs adresses séparées par des virgules. Dans ce cas, ClickHouse utilisera le traitement distribué, donc il enverra la requête à toutes les adresses spécifiées (comme les fragments avec des données différentes).

Exemple:

``` text
example01-01-1,example01-02-1
```

Une partie de l'expression peut être spécifiée entre crochets. L'exemple précédent peut être écrite comme suit:

``` text
example01-0{1,2}-1
```

Les accolades peuvent contenir une plage de Nombres séparés par deux points (entiers non négatifs). Dans ce cas, la gamme est étendue à un ensemble de valeurs qui génèrent fragment d'adresses. Si le premier nombre commence par zéro, les valeurs sont formées avec le même alignement zéro. L'exemple précédent peut être écrite comme suit:

``` text
example01-{01..02}-1
```

Si vous avez plusieurs paires d'accolades, il génère le produit direct des ensembles correspondants.

Les adresses et les parties d'adresses entre crochets peuvent être séparées par le symbole de tuyau (\|). Dans ce cas, les ensembles correspondants de adresses sont interprétés comme des répliques, et la requête sera envoyée à la première sain réplique. Cependant, les répliques sont itérées dans l'ordre actuellement défini dans [équilibrage](../../operations/settings/settings.md) paramètre.

Exemple:

``` text
example01-{01..02}-{1|2}
```

Cet exemple spécifie deux fragments qui ont chacun deux répliques.

Le nombre d'adresses générées est limitée par une constante. En ce moment, c'est 1000 adresses.

À l'aide de la `remote` la fonction de table est moins optimale que la création d'un `Distributed` table, car dans ce cas, la connexion au serveur est rétablie pour chaque requête. En outre, si des noms d'hôte, les noms sont résolus, et les erreurs ne sont pas comptés lors de travail avec diverses répliques. Lors du traitement d'un grand nombre de requêtes, créez toujours `Distributed` table à l'avance, et ne pas utiliser la `remote` table de fonction.

Le `remote` table de fonction peut être utile dans les cas suivants:

-   Accès à un serveur spécifique pour la comparaison de données, le débogage et les tests.
-   Requêtes entre différents clusters ClickHouse à des fins de recherche.
-   Demandes distribuées peu fréquentes qui sont faites manuellement.
-   Distribué demandes où l'ensemble des serveurs est redéfinie à chaque fois.

Si l'utilisateur n'est pas spécifié, `default` est utilisée.
Si le mot de passe n'est spécifié, un mot de passe vide est utilisé.

`remoteSecure` - la même chose que `remote` but with secured connection. Default port — [tcp\_port\_secure](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) de config ou 9440.

[Article Original](https://clickhouse.tech/docs/en/query_language/table_functions/remote/) <!--hide-->
