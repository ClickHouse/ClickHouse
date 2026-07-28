/**
 * SettingsInfoBlock — compact "Type / Default / Changeable" summary for a
 * setting, rendered as a lightweight two-column block.
 *
 * Mintlify equivalent of clickhouse-docs's `src/theme/SettingsInfoBlock`.
 *
 * Usage:
 *   import SettingsInfoBlock from "/snippets/components/SettingsInfoBlock/SettingsInfoBlock.jsx";
 *
 *   <SettingsInfoBlock type="Bool" default_value="0" />
 *   <SettingsInfoBlock type="String" default_value="''" changeable_without_restart="No" />
 */
const SettingsInfoBlock = ({ type, default_value, changeable_without_restart }) => {
  return (
    <div
      className="not-prose"
      style={{
        display: "flex",
        flexWrap: "wrap",
        alignItems: "baseline",
        columnGap: "0.5rem",
        rowGap: "0.125rem",
        margin: "0.375rem 0",
        fontSize: "0.8125rem",
        lineHeight: "1.125rem",
      }}
    >
      <div style={{ fontWeight: 600, opacity: 0.72 }}>Type</div>
      <div style={{ overflowWrap: "anywhere" }}>{type}</div>
      <div style={{ fontWeight: 600, opacity: 0.72, marginInlineStart: "0.5rem" }}>Default</div>
      <div style={{ overflowWrap: "anywhere" }}>{default_value}</div>
      {changeable_without_restart && (
        <div style={{ fontWeight: 600, opacity: 0.72, marginInlineStart: "0.5rem" }}>
          Changeable without restart
        </div>
      )}
      {changeable_without_restart && (
        <div style={{ overflowWrap: "anywhere" }}>
          {changeable_without_restart}
        </div>
      )}
    </div>
  );
};

export default SettingsInfoBlock;
