import { galaxyOnClick } from '/snippets/lib/galaxy.jsx';
export const BetaBadge = ({ link, galaxyTrack, galaxyEvent, label }) => {
    if (link) {
        return (
            <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="betaBadge"
                onClick={galaxyTrack && galaxyEvent ? galaxyOnClick(galaxyEvent) : undefined}
            >
                <span>{label ?? 'تجريبي'}</span>
            </a>
        )
    }

    return (
        <a
            href="https://clickhouse.com/docs/reference/settings/beta-and-experimental-features#beta-features"
            className="betaBadge"
        >
            <span>{label ?? 'ميزة تجريبية'}</span>
        </a>
    )
}
export default BetaBadge;