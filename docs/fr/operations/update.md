---
machine_translated: true
---

# Mise À Jour De ClickHouse {#clickhouse-update}

Si ClickHouse a été installé à partir de paquets deb, exécutez les commandes suivantes sur le serveur:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

Si vous avez installé ClickHouse en utilisant autre chose que les paquets deb recommandés, utilisez la méthode de mise à jour appropriée.

ClickHouse ne prend pas en charge une mise à jour distribuée. L'opération doit être effectuée consécutivement sur chaque serveur séparé. Ne pas mettre à jour tous les serveurs d'un cluster simultanément, ou le cluster sera indisponible pendant un certain temps.
