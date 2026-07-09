---
description: 'Aperçu des domaines dans ClickHouse, qui étendent les types de base avec
  des fonctionnalités supplémentaires'
sidebar_label: 'Domaines'
sidebar_position: 56
slug: /sql-reference/data-types/domains/
title: 'Domaines'
doc_type: 'reference'
---

Les domaines sont des types spécialisés qui ajoutent des fonctionnalités aux types de base existants, tout en conservant intact le format sur le fil et sur disque du type de données sous-jacent. Actuellement, ClickHouse ne prend pas en charge les domaines définis par l’utilisateur.

Vous pouvez utiliser les domaines partout où le type de base correspondant peut être utilisé, par exemple :

* Créer une colonne d’un type de domaine
* Lire/écrire des valeurs depuis/vers une colonne de domaine
* L’utiliser comme index si un type de base peut être utilisé comme index
* Appeler des fonctions avec des valeurs d’une colonne de domaine

<div id="extra-features-of-domains">
  ### Fonctionnalités supplémentaires des domaines
</div>

* Nom de type de colonne explicite dans `SHOW CREATE TABLE` ou `DESCRIBE TABLE`
* Entrée dans un format lisible par l’humain avec `INSERT INTO domain_table(domain_column) VALUES(...)`
* Sortie dans un format lisible par l’humain pour `SELECT domain_column FROM domain_table`
* Chargement de données depuis une source externe dans un format lisible par l’humain : `INSERT INTO domain_table FORMAT CSV ...`

<div id="limitations">
  ### Limitations
</div>

* Impossible de convertir une colonne d’index d’un type de base en type de domaine avec `ALTER TABLE`.
* Impossible de convertir implicitement des valeurs de type chaîne en valeurs de domaine lors de l’insertion de données depuis une autre colonne ou une autre table.
* Le domaine n’ajoute aucune contrainte aux valeurs stockées.