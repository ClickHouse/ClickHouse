---
slug: /engines/table-engines/special/view
sidebar_position: 90
sidebar_label:  View
title: "View Table Engine"
description: "Used for implementing views (for more information, see the `CREATE VIEW query`). It does not store data, but only stores the specified `SELECT` query. When reading from a table, it runs this query (and deletes all unnecessary columns from the query)."
---

# View Table Engine

Used for implementing views (for more information, see the `CREATE VIEW query`). It does not store data, but only stores the specified `SELECT` query. When reading from a table, it runs this query (and deletes all unnecessary columns from the query).
