/**
 * SettingsInfoBlock — compact "Type / Default / Changeable" summary for a
 * setting, rendered as a plain table.
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
  const cells = [
    ["Type", <Badge color="surface">{type}</Badge>],
    ["Default value", <Badge color="surface">{default_value}</Badge>],
  ];
  if (changeable_without_restart) {
    const isYes = String(changeable_without_restart).trim().toLowerCase() === "yes";
    const badge = isYes ? (
      <Badge icon="check" stroke color="green" size="sm">Yes</Badge>
    ) : (
      <Badge icon="x" stroke color="red" size="sm">No</Badge>
    );
    cells.push(["Changeable without restart", badge]);
  }
  return (
    <table>
      <thead>
        <tr>
          {cells.map(([h]) => (
            <th key={h}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        <tr>
          {cells.map(([h, v]) => (
            <td key={h}>{v}</td>
          ))}
        </tr>
      </tbody>
    </table>
  );
};

export default SettingsInfoBlock;