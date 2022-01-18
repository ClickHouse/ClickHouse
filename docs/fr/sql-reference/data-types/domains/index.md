---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Domaine
toc_priority: 56
toc_title: "Aper\xE7u"
---

# Domaine {#domains}

Les domaines sont des types spéciaux qui ajoutent des fonctionnalités supplémentaires au sommet du type de base existant, mais en laissant le format on-wire et on-disc du type de données sous-jacent intact. À l'heure actuelle, ClickHouse ne prend pas en charge les domaines définis par l'utilisateur.

Vous pouvez utiliser des domaines partout type de base correspondant peut être utilisé, par exemple:

-   Créer une colonne d'un type de domaine
-   Valeurs de lecture / écriture depuis / vers la colonne de domaine
-   L'utiliser comme un indice si un type de base peut être utilisée comme un indice
-   Fonctions d'appel avec des valeurs de colonne de domaine

### Fonctionnalités supplémentaires des domaines {#extra-features-of-domains}

-   Nom de type de colonne explicite dans `SHOW CREATE TABLE` ou `DESCRIBE TABLE`
-   Entrée du format convivial avec `INSERT INTO domain_table(domain_column) VALUES(...)`
-   Sortie au format convivial pour `SELECT domain_column FROM domain_table`
-   Chargement de données à partir d'une source externe dans un format convivial: `INSERT INTO domain_table FORMAT CSV ...`

### Limitation {#limitations}

-   Impossible de convertir la colonne d'index du type de base en type de domaine via `ALTER TABLE`.
-   Impossible de convertir implicitement des valeurs de chaîne en valeurs de domaine lors de l'insertion de données d'une autre colonne ou table.
-   Le domaine n'ajoute aucune contrainte sur les valeurs stockées.

[Article Original](https://clickhouse.tech/docs/en/data_types/domains/overview) <!--hide-->
