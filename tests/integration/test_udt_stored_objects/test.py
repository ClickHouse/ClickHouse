def physicalization_dry_run(selector):
    result = rows_json(f"PHYSICALIZE TYPE REFERENCES {selector} DRY RUN")
    assert len(result) == 1
    assert result[0]["scope_count"] == 1
    assert result[0]["manifest_count"] > 0
    assert result[0]["apply_token"]
    assert result[0]["canonical_loss_manifest_base64"]
    return result[0]
