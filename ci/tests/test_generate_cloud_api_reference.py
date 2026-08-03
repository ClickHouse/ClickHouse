from ci.jobs.scripts.docs.generate_cloud_api_reference import localize_fragment


def test_localize_fragment_preserves_names_and_syncs_pages():
    generated = [
        {
            "group": "Organization",
            "pages": [
                {"group": "Billing", "pages": ["GET /usage", "POST /invoice"]},
                {"group": "UDF", "pages": ["products/cloud/api-reference/udf/create"]},
            ],
        },
        {
            "group": "ClickStack",
            "pages": ["products/cloud/api-reference/clickstack/list-webhooks"],
        },
    ]
    current = [
        {
            "group": "Organisation traduite",
            "pages": [
                {"group": "Facturation", "pages": ["GET /usage"]},
            ],
        },
        {
            "group": "Pile de clics",
            "pages": [
                "products/cloud/api-reference/clickstack/list-connections",
                "products/cloud/api-reference/clickstack/list-webhooks",
            ],
        },
    ]

    localized = localize_fragment(generated, current)

    assert localized == [
        {
            "group": "Organisation traduite",
            "pages": [
                {"group": "Facturation", "pages": ["GET /usage", "POST /invoice"]},
                {"group": "UDF", "pages": ["products/cloud/api-reference/udf/create"]},
            ],
        },
        {
            "group": "Pile de clics",
            "pages": ["products/cloud/api-reference/clickstack/list-webhooks"],
        },
    ]


def test_localize_fragment_matches_reordered_groups_by_pages():
    generated = [
        {"group": "Second", "pages": ["GET /second"]},
        {"group": "First", "pages": ["GET /first"]},
    ]
    current = [
        {"group": "Premier", "pages": ["GET /first"]},
        {"group": "Deuxième", "pages": ["GET /second"]},
    ]

    assert localize_fragment(generated, current) == [
        {"group": "Deuxième", "pages": ["GET /second"]},
        {"group": "Premier", "pages": ["GET /first"]},
    ]
