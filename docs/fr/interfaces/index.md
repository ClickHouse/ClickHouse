---
machine_translated: true
machine_translated_rev: f865c9653f9df092694258e0ccdd733c339112f5
toc_folder_title: Interfaces
toc_priority: 14
toc_title: Introduction
---

# Interface {#interfaces}

ClickHouse fournit deux interfaces réseau (les deux peuvent être encapsulées en option dans TLS pour plus de sécurité):

-   [HTTP](http.md) qui est documenté et facile à utiliser directement.
-   [Natif de TCP](tcp.md) qui a moins de frais généraux.

Dans la plupart des cas, il est recommandé d'utiliser un outil ou une bibliothèque approprié au lieu d'interagir directement avec ceux-ci. Officiellement pris en charge par Yandex sont les suivants:

-   [Client de ligne de commande](cli.md)
-   [JDBC](jdbc.md)
-   [Pilote ODBC](odbc.md)
-   [Bibliothèque client c++ ](cpp.md)

Il existe également un large éventail de bibliothèques tierces pour travailler avec ClickHouse:

-   [Bibliothèques clientes](third-party/client_libraries.md)
-   [Intégration](third-party/integrations.md)
-   [Les interfaces visuelles](third-party/gui.md)

[Article Original](https://clickhouse.tech/docs/en/interfaces/) <!--hide-->
