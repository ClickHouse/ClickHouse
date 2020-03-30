---
machine_translated: true
---

# Caractéristiques de ClickHouse qui peuvent être considérées comme des inconvénients {#clickhouse-features-that-can-be-considered-disadvantages}

1.  Pas de transactions à part entière.
2.  Manque de capacité à modifier ou supprimer des données déjà insérées avec un taux élevé et une faible latence. Des suppressions et des mises à jour par lots sont disponibles pour nettoyer ou modifier les données, par exemple pour [GDPR](https://gdpr-info.eu).
3.  L'index clairsemé rend ClickHouse pas si approprié pour les requêtes ponctuelles récupérant des lignes simples par leurs clés.

[Article Original](https://clickhouse.tech/docs/en/introduction/features_considered_disadvantages/) <!--hide-->
